// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Sql;
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

            DataFrame lines = spark
                .ReadStream()
                .Format("kafka")
                .Option("kafka.bootstrap.servers", bootstrapServers)
                .Option(subscribeType, topics)
                .Load()
                .SelectExpr("CAST(value AS STRING)");

            DataFrame words = lines
                .Select(Explode(Split(lines["value"], " "))
                    .Alias("value"));
            DataFrame wordCounts = words.GroupBy("value").Count();
            
            //Spark.Sql.Streaming.StreamingQuery query = wordCounts
            //    .WriteStream()
            //    .OutputMode("complete")
            //    .Format("console")
            //    .Start();

            Spark.Sql.Streaming.StreamingQuery query = wordCounts
                .SelectExpr("CAST(value AS STRING)")
                .WriteStream()
                .OutputMode("complete")
                .Format("kafka")
                .Option("kafka.bootstrap.servers", "localhost:9092")
                .Option("topic", "replay-topic")
                .Option("checkpointLocation", _CheckpointLocation)
                .Start();

            query.AwaitTermination();
        }
    }
}
