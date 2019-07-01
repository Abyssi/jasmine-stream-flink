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

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.jasmine.stream.models.*;
import org.jasmine.stream.operators.CollectorAggregateFunction;
import org.jasmine.stream.operators.CounterAggregateFunction;
import org.jasmine.stream.operators.DecimalCounterAggregateFunction;
import org.jasmine.stream.operators.TopAggregateFunction;
import org.jasmine.stream.utils.BoundedPriorityQueue;
import org.jasmine.stream.utils.DateUtils;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;

import java.util.Calendar;
import java.util.Comparator;
import java.util.Objects;
import java.util.Properties;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<CommentInfo> inputStream = env
                .addSource(new FlinkKafkaConsumer<>("input-topic", new JSONClassDeserializationSchema<>(CommentInfo.class), properties))
                .filter(Objects::nonNull)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentInfo>() {
                    @Override
                    public long extractAscendingTimestamp(CommentInfo commentInfo) {
                        return commentInfo.getCreateDate();
                    }
                });

        // Query 1
        DataStream<Top3Article> top3Articles = inputStream
                .map(CommentInfo::getArticleID)
                .keyBy(s -> s)
                .timeWindow(Time.hours(1))
                .aggregate(new CounterAggregateFunction<>())
                .timeWindowAll(Time.hours(1))
                .aggregate(new TopAggregateFunction<Tuple2<String, Long>>() {
                    @Override
                    public BoundedPriorityQueue<Tuple2<String, Long>> createAccumulator() {
                        return new BoundedPriorityQueue<>(3, Comparator.comparingLong(value -> value.f1));
                    }
                })
                .map(item -> {
                    Tuple2<String, Long>[] array = item.toArray();
                    return new Top3Article(0, array[0].f0, array[0].f1, array[1].f0, array[1].f1, array[2].f0, array[2].f1);
                });

        // Query 2
        DataStream<CommentHourlyCount> commentsCount = inputStream
                .filter(item -> item.getCommentType() == CommentType.COMMENT)
                .map(item -> {
                    Calendar calendar = DateUtils.parseCalendar(item.getCreateDate());
                    return (int) Math.floor(calendar.get(Calendar.HOUR_OF_DAY) % 2.0);
                })
                .keyBy(s -> s)
                .timeWindow(Time.hours(24))
                .aggregate(new CounterAggregateFunction<>())
                .timeWindowAll(Time.hours(24))
                .aggregate(new CollectorAggregateFunction<>())
                .map(item -> new CommentHourlyCount(0, item.get(0), item.get(1), item.get(2), item.get(3), item.get(4), item.get(5), item.get(6), item.get(7), item.get(8), item.get(9), item.get(10), item.get(11)));

        // Query 3
        DataStream<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>> likesCount = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple4<>(item.getUserID(), item.getRecommendations(), item.isEditorsSelection(), item.getUserDisplayName()))
                .keyBy(item -> item.f0)
                .timeWindow(Time.hours(24))
                .aggregate(new DecimalCounterAggregateFunction<Tuple4<Long, Long, Boolean, String>>() {
                    @Override
                    public Tuple2<Tuple4<Long, Long, Boolean, String>, Double> add(Tuple4<Long, Long, Boolean, String> e, Tuple2<Tuple4<Long, Long, Boolean, String>, Double> tuple4DoubleTuple2) {
                        tuple4DoubleTuple2.f1 += e.f1 * (e.f2 ? 1.1 : 1);
                        return tuple4DoubleTuple2;
                    }
                });

        DataStream<Tuple2<String, Long>> indirectCommentsCount = inputStream
                .filter(item -> item.getDepth() > 1)
                .map(CommentInfo::getParentUserDisplayName)
                .keyBy(s -> s)
                .timeWindow(Time.hours(24))
                .aggregate(new CounterAggregateFunction<>());

        double wa = 0.3;
        double wb = 0.7;
        DataStream<TopUserRatings> topUserRatings = likesCount.join(indirectCommentsCount)
                .where(item -> item.f0.f3).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply((JoinFunction<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>, Tuple2<String, Long>, Tuple2<Long, Double>>) (tuple4DoubleTuple2, stringLongTuple2) -> new Tuple2<>(tuple4DoubleTuple2.f0.f0, wa*tuple4DoubleTuple2.f1 + wb*stringLongTuple2.f1))
                .timeWindowAll(Time.hours(1))
                .aggregate(new TopAggregateFunction<Tuple2<Long, Double>>() {
                    @Override
                    public BoundedPriorityQueue<Tuple2<Long, Double>> createAccumulator() {
                        return new BoundedPriorityQueue<>(3, Comparator.comparingDouble(value -> value.f1));
                    }
                }).map(item -> {
                    Tuple2<Long, Double>[] array = item.toArray();
                    return new TopUserRatings(0, array[0].f0, array[0].f1, array[1].f0, array[1].f1, array[2].f0, array[2].f1, array[3].f0, array[3].f1, array[4].f0, array[4].f1, array[5].f0, array[5].f1, array[6].f0, array[6].f1, array[7].f0, array[7].f1, array[8].f0, array[8].f1, array[9].f0, array[9].f1);
                });

        //inputStream.print();
        top3Articles.print();
        commentsCount.print();
        topUserRatings.print();

        // execute program
        env.execute("JASMINE Stream");
    }
}
