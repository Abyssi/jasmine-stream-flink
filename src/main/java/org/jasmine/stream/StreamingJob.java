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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.jasmine.stream.config.Configuration;
import org.jasmine.stream.models.CommentHourlyCount;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.Top3Article;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.queries.CommentsCountQuery;
import org.jasmine.stream.queries.Top3ArticlesQuery;
import org.jasmine.stream.queries.TopUserRatingsQuery;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;
import org.jasmine.stream.utils.JSONClassSerializationSchema;

import java.util.Objects;
import java.util.Properties;

public class StreamingJob {

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        //inputStream.print();

        // Query 1
        //DataStream<Top3Article> top3Articles1h = Top3ArticlesQuery.run(inputStream, Time.hours(1));
        //DataStream<Top3Article> top3Articles24h = Top3ArticlesQuery.run(inputStream, Time.hours(24));
        //DataStream<Top3Article> top3Articles7d = Top3ArticlesQuery.run(inputStream, Time.days(7));

        Tuple3<DataStream<Top3Article>, DataStream<Top3Article>, DataStream<Top3Article>> top3ArticlesStreams = Top3ArticlesQuery.runAll(inputStream);
        DataStream<Top3Article> top3Articles1h = top3ArticlesStreams.f0;
        DataStream<Top3Article> top3Articles24h = top3ArticlesStreams.f1;
        DataStream<Top3Article> top3Articles7d = top3ArticlesStreams.f2;

        top3Articles1h.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "top3Articles1h"), new JSONClassSerializationSchema<>(), properties));
        top3Articles24h.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "top3Articles24h"), new JSONClassSerializationSchema<>(), properties));
        top3Articles7d.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "top3Articles7d"), new JSONClassSerializationSchema<>(), properties));

        top3Articles1h.print();
        top3Articles24h.print();
        top3Articles7d.print();


        // Query 2
        //DataStream<CommentHourlyCount> commentsCount24h = CommentsCountQuery.run(inputStream, Time.hours(24));
        //DataStream<CommentHourlyCount> commentsCount7d = CommentsCountQuery.run(inputStream, Time.days(7));
        //DataStream<CommentHourlyCount> commentsCount1M = CommentsCountQuery.run(inputStream, Time.days(30));

        Tuple3<DataStream<CommentHourlyCount>, DataStream<CommentHourlyCount>, DataStream<CommentHourlyCount>> commentsCountStreams = CommentsCountQuery.runAll(inputStream);
        DataStream<CommentHourlyCount> commentsCount24h = commentsCountStreams.f0;
        DataStream<CommentHourlyCount> commentsCount7d = commentsCountStreams.f1;
        DataStream<CommentHourlyCount> commentsCount1M = commentsCountStreams.f2;

        commentsCount24h.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "commentsCount24h"), new JSONClassSerializationSchema<>(), properties));
        commentsCount7d.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "commentsCount7d"), new JSONClassSerializationSchema<>(), properties));
        commentsCount1M.addSink(new FlinkKafkaProducer<>(String.format(Configuration.getParameters().get("kafka-output-topic"), "commentsCount1M"), new JSONClassSerializationSchema<>(), properties));

        commentsCount24h.print();
        commentsCount7d.print();
        commentsCount1M.print();


        // Query 3
        //DataStream<Top3Article> topUserRatings24h = Top3ArticlesQuery.run(inputStream, Time.hours(1));
        //DataStream<Top3Article> topUserRatings7d = Top3ArticlesQuery.run(inputStream, Time.hours(24));
        //DataStream<Top3Article> topUserRatings1M = Top3ArticlesQuery.run(inputStream, Time.days(7));

        Tuple3<DataStream<TopUserRatings>, DataStream<TopUserRatings>, DataStream<TopUserRatings>> topUserRatingsStreams = TopUserRatingsQuery.runAll(inputStream);
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
