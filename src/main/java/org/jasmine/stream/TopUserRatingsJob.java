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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.jasmine.stream.config.Configuration;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.queries.TopUserRatingsQuery;
import org.jasmine.stream.utils.JNStreamExecutionEnvironment;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;
import org.jasmine.stream.utils.JSONClassSerializationSchema;

import java.util.Objects;
import java.util.Properties;

public class TopUserRatingsJob {

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = JNStreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", Configuration.getParameters().get("kafka-server"));
        properties.setProperty("group.id", Configuration.getParameters().get("kafka-group-id"));
        properties.setProperty("auto.offset.reset", "latest");

        DataStream<CommentInfo> inputStream = env
                .addSource(new FlinkKafkaConsumer<>(Configuration.getParameters().get("kafka-input-topic"), new JSONClassDeserializationSchema<>(CommentInfo.class), properties))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentInfo>() {
                    @Override
                    public long extractAscendingTimestamp(CommentInfo commentInfo) {
                        return commentInfo.getCreateDate() * 1000;
                    }
                });

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost(Configuration.getParameters().get("redis-hostname"))
                .setPort(Integer.valueOf(Configuration.getParameters().get("redis-port")))
                .build();

        // Query 3
        //DataStream<TopUserRatings> topUserRatings24h = TopUserRatingsQuery.run(jedisPoolConfig, inputStream, Time.hours(24));
        //DataStream<TopUserRatings> topUserRatings7d = TopUserRatingsQuery.run(jedisPoolConfig, inputStream, Time.hours(24));
        //DataStream<TopUserRatings> topUserRatings1M = TopUserRatingsQuery.run(jedisPoolConfig, inputStream, Time.days(7));

        Tuple3<DataStream<TopUserRatings>, DataStream<TopUserRatings>, DataStream<TopUserRatings>> topUserRatingsStreams = TopUserRatingsQuery.runAll(jedisPoolConfig, inputStream);
        DataStream<TopUserRatings> topUserRatings24h = topUserRatingsStreams.f0;
        DataStream<TopUserRatings> topUserRatings7d = topUserRatingsStreams.f1;
        DataStream<TopUserRatings> topUserRatings1M = topUserRatingsStreams.f2;

        topUserRatings24h.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "topUserRatings24h"), new JSONClassSerializationSchema<>(), properties));
        topUserRatings7d.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "topUserRatings7d"), new JSONClassSerializationSchema<>(), properties));
        topUserRatings1M.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "topUserRatings1M"), new JSONClassSerializationSchema<>(), properties));

        topUserRatings24h.print();
        topUserRatings7d.print();
        topUserRatings1M.print();

        // execute program
        env.execute("JASMINE Stream");
    }
}
