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
        private static readonly string _CheckpointLocation = @"C:\temp\sparkcheckpoint";
        private static readonly string _Value = "value";
        private static readonly string[] _ReturnColumns = { "ID", "EventTypeID", "UID1", "UID2", "objectType", "Timestamp", "ManualTimestamp", "KafkasorgTimestamp" };

        private static readonly string _BootstrapServers = "localhost:9092";
        private static readonly string _SubscribeType = "subscribe";

        public void Run(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("StructuredKafkaWordCount")
                .Config("checkpointLocation", _CheckpointLocation)
                .GetOrCreate();

            DataFrame loginsStream = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", _BootstrapServers)
                .Option(_SubscribeType, "test-topic")
                .Load()
                .SelectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)");
            loginsStream.PrintSchema();

            DataFrame logins = GetDeserializedDF(loginsStream);

            Spark.Sql.Streaming.StreamingQuery query = logins
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

        private DataFrame GetDeserializedDF(DataFrame p_RawDF)
        {
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
            objectDF = objectDF.WithColumn("ManualTimestamp", ToTimestamp(Col($"{_Value}.Timestamp")));
            objectDF.PrintSchema();

            DataFrame returnColumns = objectDF.Select(GetReturnColumnNamesWithPrefix()[0], GetReturnColumnNamesWithPrefix().ToArray().Skip(1).ToArray());
            returnColumns.PrintSchema();

            DataFrame returnColumnsJson = returnColumns.Select(ToJson(Struct("*")).Alias(_Value));
            returnColumnsJson.PrintSchema();

            return returnColumnsJson;
        }

        private string[] GetReturnColumnNamesWithPrefix()
        {
            string[] res = new string[_ReturnColumns.Length];
            for (var i = 0; i < _ReturnColumns.Length; i++)
            {
                string col = _ReturnColumns[i];

                if (!col.ToUpper().Contains("KAFKASORG") && !col.ToUpper().Contains("MANUAL"))
                {
                    res[i] = $"{_Value}.{col}";
                }
                else
                {
                    res[i] = col;
                }
            }

            return res;
        }
    }
}
