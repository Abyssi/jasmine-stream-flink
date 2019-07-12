/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jasmine.stream;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.jasmine.stream.config.FlinkConfiguration;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.queries.TopUserRatingsQuery;
import org.jasmine.stream.utils.JNStreamExecutionEnvironment;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;
import org.jasmine.stream.utils.JSONClassSerializationSchema;
import org.jasmine.stream.utils.LatencyTracker;

import java.io.File;
import java.util.Objects;
import java.util.Properties;

public class TopUserRatingsJob {

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = JNStreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", FlinkConfiguration.getParameters().get("kafka-server"));
        properties.setProperty("group.id", FlinkConfiguration.getParameters().get("kafka-group-id"));
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<CommentInfo> inputStream = env
                .addSource(new FlinkKafkaConsumer<>(FlinkConfiguration.getParameters().get("kafka-input-topic"), new JSONClassDeserializationSchema<>(CommentInfo.class), properties))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentInfo>() {
                    @Override
                    public long extractAscendingTimestamp(CommentInfo commentInfo) {
                        return commentInfo.getCreateDate() * 1000;
                    }
                });

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost(FlinkConfiguration.getParameters().get("redis-hostname"))
                .setPort(Integer.valueOf(FlinkConfiguration.getParameters().get("redis-port")))
                .build();

        FileUtils.deleteDirectory(new File("output"));

        // Query 3
        //DataStream<TopUserRatings> topUserRatings24h = TopUserRatingsQuery.run(jedisPoolConfig, inputStream, Time.hours(24));
        //DataStream<TopUserRatings> topUserRatings7d = TopUserRatingsQuery.run(jedisPoolConfig, inputStream, Time.hours(24));
        //DataStream<TopUserRatings> topUserRatings1M = TopUserRatingsQuery.run(jedisPoolConfig, inputStream, Time.days(7));

        Tuple3<DataStream<TopUserRatings>, DataStream<TopUserRatings>, DataStream<TopUserRatings>> topUserRatingsStreams = TopUserRatingsQuery.runAll(jedisPoolConfig, inputStream);
        DataStream<TopUserRatings> topUserRatings24h = topUserRatingsStreams.f0;
        DataStream<TopUserRatings> topUserRatings7d = topUserRatingsStreams.f1;
        DataStream<TopUserRatings> topUserRatings1M = topUserRatingsStreams.f2;

        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, topUserRatings24h).getEndStream().print("topUserRatings24h");
        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, topUserRatings7d).getEndStream().print("topUserRatings7d");
        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, topUserRatings1M).getEndStream().print("topUserRatings1M");

        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            topUserRatings24h.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "topUserRatings24h"), new JSONClassSerializationSchema<>(), properties));
        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            topUserRatings7d.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "topUserRatings7d"), new JSONClassSerializationSchema<>(), properties));
        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            topUserRatings1M.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "topUserRatings1M"), new JSONClassSerializationSchema<>(), properties));

        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) topUserRatings24h.print();
        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) topUserRatings7d.print();
        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) topUserRatings1M.print();

        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            topUserRatings24h.writeAsText("output/topUserRatings24h.json").setParallelism(1);
        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            topUserRatings7d.writeAsText("output/topUserRatings7d.json").setParallelism(1);
        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            topUserRatings1M.writeAsText("output/topUserRatings1M.json").setParallelism(1);

        // execute program
        env.execute("JASMINE Stream");
    }
}
