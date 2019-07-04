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

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.jasmine.stream.models.CommentHourlyCount;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.Top3Article;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.queries.CommentsCountQuery;
import org.jasmine.stream.queries.Top3ArticlesQuery;
import org.jasmine.stream.queries.TopUserRatingsQuery;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;

import java.util.Objects;
import java.util.Properties;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<CommentInfo> inputStream = env
                .addSource(new FlinkKafkaConsumer<>("jasmine_input_topic", new JSONClassDeserializationSchema<>(CommentInfo.class), properties))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentInfo>() {
                    @Override
                    public long extractAscendingTimestamp(CommentInfo commentInfo) {
                        return commentInfo.getCreateDate() * 1000;
                    }
                });

        //inputStream.map(new PeekMapFunction<>());

        // Query 1
        DataStream<Top3Article> top3Articles1h = Top3ArticlesQuery.run(inputStream, Time.hours(1));
        //DataStream<Top3Article> top3Articles24h = Top3ArticlesQuery.run(inputStream, Time.hours(24));
        //DataStream<Top3Article> top3Articles7d = Top3ArticlesQuery.run(inputStream, Time.days(7));

        // Query 2
        //DataStream<CommentHourlyCount> commentsCount24h = CommentsCountQuery.run(inputStream, Time.hours(24));
        //DataStream<CommentHourlyCount> commentsCount7d = CommentsCountQuery.run(inputStream, Time.days(7));
        //DataStream<CommentHourlyCount> commentsCount1m = CommentsCountQuery.run(inputStream, Time.days(30));

        // Query 3
        //DataStream<TopUserRatings> topUserRatings24h = TopUserRatingsQuery.run(inputStream, Time.hours(24));
        //DataStream<TopUserRatings> topUserRatings7d = TopUserRatingsQuery.run(inputStream, Time.days(7));
        //DataStream<TopUserRatings> topUserRatings1m = TopUserRatingsQuery.run(inputStream, Time.days(30));

        //inputStream.print();
        top3Articles1h.print();
        //commentsCount24h.print();
        //topUserRatings24h.print();

        // execute program
        env.execute("JASMINE Stream");
    }
}
