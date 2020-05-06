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
        private static readonly string _IP = "192.168.1.80";
        private static readonly string _KafkaPort = "9092";
        private static readonly string _BootstrapServers = $"{_IP}:{_KafkaPort}";
        private static readonly string _SparkDriverPort = "61761";
        private static readonly string _SubscribeType = "subscribe";
        private static readonly string _CheckpointLocationSpark = @"C:\temp\sparkcheckpoint\spark";
        private static readonly string _CheckpointLocationQuery = @"C:\temp\sparkcheckpoint\query";
        private static readonly string _RecoveryDirectory = @"C:\temp\sparkcheckpoint\recoverydir";
        private static readonly string _Value = "value";

        public void Run(string[] args)
        {
            // TODO M: Read from Kafka API and not as Consumer (createDirectStream instead of createStream): https://spark.apache.org/docs/latest/streaming-programming-guide.html?fbclid=IwAR2Jjhls4VDOQlrnuMdHb0FN-it69a7jBzfjmd8OLtWDJC7BeDFBpxPKoys#with-kafka-direct-api ; https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
            // TODO M: Manual offset commiting: https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html#obtaining-offsets -> https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/kafka
            // TODO M: Deployment: https://spark.apache.org/docs/latest/streaming-programming-guide.html?fbclid=IwAR2Jjhls4VDOQlrnuMdHb0FN-it69a7jBzfjmd8OLtWDJC7BeDFBpxPKoys#deploying-applications
            
            // TODO M: Deploy Spark App in Cluster mode. (How to deploy Spark app to a cluster, close the terminal and the app keeps running?)
            // TODO M: How to stop Spark app Gracefully? How to stop Query.awaitTermination? Create touch file. : https://www.linkedin.com/pulse/how-shutdown-spark-streaming-job-gracefully-lan-jiang/

            SparkSession spark = SparkSession
                .Builder()
                .AppName("KafkaStreamJoins")
                .Config("spark.driver.host", _IP)
                .Config("spark.driver.port", _SparkDriverPort)
                //.Config("spark.executor.instances", "1")
                .Config("checkpointLocation", _CheckpointLocationSpark)
                .Config("spark.deploy.recoveryMode", "FILESYSTEM")
                .Config("spark.deploy.recoveryDirectory", _RecoveryDirectory)
                .Config("spark.streaming.backpressure", "true")
                .Config("spark.streaming.backpressure.pid.minRate", "10")
                .Config("spark.streaming.backpressure.initialRate", "30")
                .Config("spark.streaming.stopGracefullyOnShutdown", "true")
                //.Config("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()")
                //.Config("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem.class.getName()")
                .GetOrCreate();
            
            DataFrame logins = ReadKafkaStream(spark, "logins-topic", true);
            DataFrame logouts = ReadKafkaStream(spark, "logouts-topic", false);
            
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
                .WriteStream()
                //.OutputMode("complete")
                .Format("kafka")
                .Option("kafka.bootstrap.servers", _BootstrapServers)
                .Option("topic", "replay-topic")
                .Option("checkpointLocation", _CheckpointLocationQuery)
                .Start();

            query.AwaitTermination();
        }

        private DataFrame ReadKafkaStream(SparkSession p_SparkSession, string p_KafkaTopic, bool p_Login)
        {
            DataFrame stream = p_SparkSession
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", _BootstrapServers)
                .Option(_SubscribeType, p_KafkaTopic)
                //.Option("group.id", "spark-demo-logins")
                //.Option("enable.auto.commit", "false")
                //.Option("auto.offset.reset", "latest")
                .Option("spark.streaming.receiver.writeAheadLog.enable", "true")
                .Load()
                .SelectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
            stream.PrintSchema();
            return GetPurifiedDF(stream, p_Login);
        }

        private DataFrame GetPurifiedDF(DataFrame p_RawDF, bool p_Login = true, bool p_ToJsonValue = false)
        {
            string eventType = p_Login ? "Login" : "Logout";
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

            DataFrame objectDF = p_RawDF.Select(FromJson(Col(_Value), inputSchema.Json).Alias(_Value), Col("Timestamp").Alias("KafkasorgTimestamp"));
            objectDF = objectDF.WithColumn(manualTimestamp, ToTimestamp(Col($"{_Value}.Timestamp")));
            objectDF.PrintSchema();

            List<StructField> structFields = inputSchema.Fields;
            List<string> returnCols = new List<string> { manualTimestamp };
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

            return p_ToJsonValue ? GetJsonValueOfDF(columnsWithWatermark) : columnsWithWatermark;
        }

        private DataFrame GetJsonValueOfDF(DataFrame p_DataFrame)
        {
            DataFrame returnColumnsJson = p_DataFrame.Select(ToJson(Struct("*")).Alias(_Value));
            returnColumnsJson.PrintSchema();
            return returnColumnsJson;
        }
    }
}
