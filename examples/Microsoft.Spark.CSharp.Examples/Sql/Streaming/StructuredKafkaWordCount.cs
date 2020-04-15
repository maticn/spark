// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace Microsoft.Spark.Examples.Sql.Streaming
{
    /// <summary>
    /// The example is taken/modified from
    /// spark/examples/src/main/python/sql/streaming/structured_kafka_wordcount.py
    /// </summary>
    internal sealed class StructuredKafkaWordCount : IExample
    {
        private static readonly string _CheckpointLocation = @"C:\temp\sparkcheckpoint";
        private static readonly string _Value = "value";

        public void Run(string[] args)
        {
            if (args.Length != 3)
            {
                Console.Error.WriteLine(
                    "Usage: StructuredKafkaWordCount " +
                    "<bootstrap-servers> <subscribe-type> <topics>");
                Environment.Exit(1);
            }

            string bootstrapServers = args[0];
            string subscribeType = args[1];
            string topics = args[2];

            SparkSession spark = SparkSession
                .Builder()
                .AppName("StructuredKafkaWordCount")
                .Config("checkpointLocation", _CheckpointLocation)
                .GetOrCreate();


            // Need to explicitly specify the schema since pickling vs. arrow formatting
            // will return different types. Pickling will turn longs into ints if the values fit.
            // Same as the "age INT, name STRING" DDL-format string.
            StructType inputSchema = new StructType(new[]
            {
                new StructField("ID", new IntegerType()),
                new StructField("EventTypeID", new IntegerType()),
                new StructField("Timestamp", new TimestampType()),
                new StructField("UID1", new StringType()),
                new StructField("UID2", new StringType()),
                new StructField("objectType", new StringType()),
            });
            //DataFrame df = spark.Read().Schema(inputSchema).Json(args[0]);

            string returnColumn1 = "EventTypeID";
            string returnColumn2 = "UID2";

            DataFrame jsonString = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", bootstrapServers)
                .Option(subscribeType, topics)
                .Load()
                .SelectExpr("CAST(value AS STRING)");
            jsonString.PrintSchema();

            DataFrame objectDF = jsonString.Select(FromJson(Col(_Value), inputSchema.Json).Alias(_Value));
            objectDF.PrintSchema();

            DataFrame returnColumns = objectDF.Select($"{_Value}.{returnColumn1}", $"{_Value}.{returnColumn2}");
            returnColumns.PrintSchema();

            //DataFrame renamedColumn = returnColumns.WithColumnRenamed(returnColumn1, _Value);
            //renamedColumn.PrintSchema();

            DataFrame returnColumnsJson = returnColumns.Select(ToJson(Struct(returnColumn1, returnColumn2)).Alias(_Value));
            returnColumnsJson.PrintSchema();


            //convertedJson
            //    .WriteStream()
            //    .Format("parquet")
            //    .Option("path", "/example/streamingtripdata")
            //    .Option("checkpointLocation", "/streamcheckpoint")
            //    .Start().AwaitTermination(30000);


            Spark.Sql.Streaming.StreamingQuery query = returnColumnsJson
                .SelectExpr("CAST(value AS STRING)")
                //.SelectExpr("CAST(value AS STRING)", "CAST(UID2 AS STRING)")
                .WriteStream()
                //.OutputMode("complete")
                .Format("kafka")
                .Option("kafka.bootstrap.servers", "localhost:9092")
                .Option("topic", "replay-topic")
                .Option("checkpointLocation", _CheckpointLocation)
                .Start();

            query.AwaitTermination();
        }
    }
}
