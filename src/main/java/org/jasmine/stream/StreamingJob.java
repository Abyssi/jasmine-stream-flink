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
import org.jasmine.stream.models.CommentHourlyCount;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.Top3Article;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.queries.CommentsCountQuery;
import org.jasmine.stream.queries.TopArticlesQuery;
import org.jasmine.stream.queries.TopUserRatingsQuery;
import org.jasmine.stream.utils.JNStreamExecutionEnvironment;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;
import org.jasmine.stream.utils.JSONClassSerializationSchema;
import org.jasmine.stream.utils.LatencyTracker;

import java.io.File;
import java.util.Objects;
import java.util.Properties;

public class StreamingJob {

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

        //inputStream.print();

        FileUtils.deleteDirectory(new File("output"));

        // Query 1
        //DataStream<Top3Article> topArticles1h = TopArticlesQuery.run(inputStream, Time.hours(1));
        //DataStream<Top3Article> topArticles24h = TopArticlesQuery.run(inputStream, Time.hours(24));
        //DataStream<Top3Article> topArticles7d = TopArticlesQuery.run(inputStream, Time.days(7));

        Tuple3<DataStream<Top3Article>, DataStream<Top3Article>, DataStream<Top3Article>> topArticlesStreams = TopArticlesQuery.runAll(inputStream);
        DataStream<Top3Article> topArticles1h = topArticlesStreams.f0;
        DataStream<Top3Article> topArticles24h = topArticlesStreams.f1;
        DataStream<Top3Article> topArticles7d = topArticlesStreams.f2;

        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, topArticles1h).getEndStream().print("topArticles1h");
        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, topArticles24h).getEndStream().print("topArticles24h");
        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, topArticles7d).getEndStream().print("topArticles7d");

        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            topArticles1h.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "topArticles1h"), new JSONClassSerializationSchema<>(), properties));
        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            topArticles24h.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "topArticles24h"), new JSONClassSerializationSchema<>(), properties));
        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            topArticles7d.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "topArticles7d"), new JSONClassSerializationSchema<>(), properties));

        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) topArticles1h.print();
        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) topArticles24h.print();
        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) topArticles7d.print();

        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            topArticles1h.writeAsText("output/topArticles1h.json").setParallelism(1);
        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            topArticles24h.writeAsText("output/topArticles24h.json").setParallelism(1);
        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            topArticles7d.writeAsText("output/topArticles7d.json").setParallelism(1);

        // Query 2
        //DataStream<CommentHourlyCount> commentsCount24h = CommentsCountQuery.run(inputStream, Time.hours(24));
        //DataStream<CommentHourlyCount> commentsCount7d = CommentsCountQuery.run(inputStream, Time.days(7));
        //DataStream<CommentHourlyCount> commentsCount1M = CommentsCountQuery.run(inputStream, Time.days(30));

        Tuple3<DataStream<CommentHourlyCount>, DataStream<CommentHourlyCount>, DataStream<CommentHourlyCount>> commentsCountStreams = CommentsCountQuery.runAll(inputStream);
        DataStream<CommentHourlyCount> commentsCount24h = commentsCountStreams.f0;
        DataStream<CommentHourlyCount> commentsCount7d = commentsCountStreams.f1;
        DataStream<CommentHourlyCount> commentsCount1M = commentsCountStreams.f2;

        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, commentsCount24h).getEndStream().print("commentsCount24h");
        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, commentsCount7d).getEndStream().print("commentsCount7d");
        if (FlinkConfiguration.getParameters().getBoolean("latency-enabled"))
            new LatencyTracker(inputStream, commentsCount1M).getEndStream().print("commentsCount1M");

        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            commentsCount24h.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "commentsCount24h"), new JSONClassSerializationSchema<>(), properties));
        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            commentsCount7d.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "commentsCount7d"), new JSONClassSerializationSchema<>(), properties));
        if (FlinkConfiguration.getParameters().getBoolean("kafka-enabled"))
            commentsCount1M.addSink(new FlinkKafkaProducer<>(String.format(FlinkConfiguration.getParameters().get("kafka-output-topic"), "commentsCount1M"), new JSONClassSerializationSchema<>(), properties));

        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) commentsCount24h.print();
        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) commentsCount7d.print();
        if (FlinkConfiguration.getParameters().getBoolean("print-enabled")) commentsCount1M.print();

        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            commentsCount24h.writeAsText("output/commentsCount24h.json").setParallelism(1);
        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            commentsCount7d.writeAsText("output/commentsCount7d.json").setParallelism(1);
        if (FlinkConfiguration.getParameters().getBoolean("write-enabled"))
            commentsCount1M.writeAsText("output/commentsCount1M.json").setParallelism(1);

        // Query 3
        //DataStream<Top3Article> topUserRatings24h = TopArticlesQuery.run(jedisPoolConfig, inputStream, Time.hours(1));
        //DataStream<Top3Article> topUserRatings7d = TopArticlesQuery.run(jedisPoolConfig, inputStream, Time.hours(24));
        //DataStream<Top3Article> topUserRatings1M = TopArticlesQuery.run(jedisPoolConfig, inputStream, Time.days(7));

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
