// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql.Streaming
{
    /// <summary>
    /// The example is taken/modified from
    /// spark/examples/src/main/python/sql/streaming/structured_kafka_wordcount.py
    /// </summary>
    internal sealed class KafkaStreamJoins : IExample
    {
        private static readonly string _BootstrapServers = "localhost:9092";
        private static readonly string _SubscribeType = "subscribe";
        private static readonly string _CheckpointLocation = @"C:\temp\sparkcheckpoint";
        private static readonly string _Value = "value";

        public void Run(string[] args)
        {
            // TODO M: Checkpointing: https://spark.apache.org/docs/latest/streaming-programming-guide.html?fbclid=IwAR2Jjhls4VDOQlrnuMdHb0FN-it69a7jBzfjmd8OLtWDJC7BeDFBpxPKoys#how-to-configure-checkpointing
            // TODO M: Reliable receiver: https://spark.apache.org/docs/latest/streaming-custom-receivers.html
            // TODO M: Read from Kafka API and not as Consumer: https://spark.apache.org/docs/latest/streaming-programming-guide.html?fbclid=IwAR2Jjhls4VDOQlrnuMdHb0FN-it69a7jBzfjmd8OLtWDJC7BeDFBpxPKoys#with-kafka-direct-api ; https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
            // TODO M: Deployment: https://spark.apache.org/docs/latest/streaming-programming-guide.html?fbclid=IwAR2Jjhls4VDOQlrnuMdHb0FN-it69a7jBzfjmd8OLtWDJC7BeDFBpxPKoys#deploying-applications


            SparkSession spark = SparkSession
                .Builder()
                .AppName("StructuredKafkaWordCount")
                //.Config("checkpointLocation", _CheckpointLocation)
                .GetOrCreate();

            DataFrame loginsStream = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", _BootstrapServers)
                .Option(_SubscribeType, "logins-topic")
                .Load()
                .SelectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
            loginsStream.PrintSchema();
            DataFrame logins = GetPurifiedDF(loginsStream, true);

            DataFrame logoutsStream = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", _BootstrapServers)
                .Option(_SubscribeType, "logouts-topic")
                .Load()
                .SelectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
            logoutsStream.PrintSchema();
            DataFrame logouts = GetPurifiedDF(logoutsStream, false);
            
            DataFrame joins = logins.Join(logouts, 
                Expr("LoginUID1 = LogoutUID1 AND LoginUID2 = LogoutUID2 AND ManualTimestampLogout >= ManualTimestampLogin AND ManualTimestampLogoutUnixInt <= ManualTimestampLoginUnixInt + interval 8 hours"));
            joins.PrintSchema();

            DataFrame droppedDuplicates = joins.DropDuplicates("LoginID");

            DataFrame timeDifference = droppedDuplicates.WithColumn("DiffInSeconds", UnixTimestamp(Col("ManualTimestampLogout")) - UnixTimestamp(Col("ManualTimestampLogin")));
            timeDifference = timeDifference.WithColumn("ResultTimestamp", CurrentTimestamp());
            timeDifference.PrintSchema();

            DataFrame result = GetJsonValueOfDF(timeDifference);

            Spark.Sql.Streaming.StreamingQuery query = result
                .SelectExpr("CAST(value AS STRING)")
                //.SelectExpr("CAST(value AS STRING)", "CAST(UID2 AS STRING)")
                .WriteStream()
                //.OutputMode("complete")
                .Format("kafka")
                .Option("kafka.bootstrap.servers", _BootstrapServers)
                .Option("topic", "replay-topic")
                .Option("checkpointLocation", _CheckpointLocation)
                .Start();

            query.AwaitTermination();
        }

        private DataFrame GetPurifiedDF(DataFrame p_rawDF, bool p_login = true, bool p_toJsonValue = false)
        {
            string eventType = p_login ? "Login" : "Logout";
            string manualTimestamp = "ManualTimestamp" + eventType;

            // Need to explicitly specify the schema since pickling vs. arrow formatting
            // will return different types. Pickling will turn longs into ints if the values fit.
            // Same as the "age INT, name STRING" DDL-format string.
            StructType inputSchema = new StructType(new[]
            {
                new StructField("ID", new IntegerType()),
                new StructField("EventTypeID", new IntegerType()),
                new StructField("Timestamp", new StringType()),
                new StructField("UID1", new StringType()),
                new StructField("UID2", new StringType()),
                new StructField("objectType", new StringType()),
            });
            //DataFrame df = spark.Read().Schema(inputSchema).Json(args[0]);

            DataFrame objectDF = p_rawDF.Select(FromJson(Col(_Value), inputSchema.Json).Alias(_Value), Col("Timestamp").Alias("KafkasorgTimestamp"));
            objectDF = objectDF.WithColumn(manualTimestamp, ToTimestamp(Col($"{_Value}.Timestamp")));
            objectDF.PrintSchema();

            List<StructField> structFields = inputSchema.Fields;
            List<string> returnCols = new List<string> {manualTimestamp};
            foreach (StructField filed in structFields)
            {
                returnCols.Add($"{_Value}.{filed.Name}");
            }
            DataFrame returnColumns = objectDF.Select("KafkasorgTimestamp", returnCols.ToArray());
            returnColumns.PrintSchema();

            DataFrame columnsWithWatermark = returnColumns
                .WithColumn(eventType + "ID", Col("ID"))
                .WithColumn(eventType + "UID1", Col("UID1"))
                .WithColumn(eventType + "UID2", Col("UID2"))
                .WithColumn(manualTimestamp + "UnixInt", FromUnixTime(UnixTimestamp(Col(manualTimestamp)).Cast("long")))
                .WithWatermark(manualTimestamp, "1 minute");
            columnsWithWatermark.PrintSchema();

            return p_toJsonValue ? GetJsonValueOfDF(columnsWithWatermark) : columnsWithWatermark;
        }

        private DataFrame GetJsonValueOfDF(DataFrame p_dataFrame)
        {
            DataFrame returnColumnsJson = p_dataFrame.Select(ToJson(Struct("*")).Alias(_Value));
            returnColumnsJson.PrintSchema();
            return returnColumnsJson;
        }
    }
}
