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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.Top3Article;
import org.jasmine.stream.operators.CounterAggregateFunction;
import org.jasmine.stream.operators.TopAggregateFunction;
import org.jasmine.stream.utils.JSONClassDeserializationSchema;
import org.jasmine.stream.utils.SerializableCallback;

import java.util.Comparator;
import java.util.Objects;
import java.util.Properties;
import java.util.function.ToLongFunction;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		DataStream<CommentInfo> stream = env
				.addSource(new FlinkKafkaConsumer<>("input-topic", new JSONClassDeserializationSchema<>(CommentInfo.class), properties))
				.filter(Objects::nonNull)
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CommentInfo>() {
					@Override
					public long extractAscendingTimestamp(CommentInfo commentInfo) {
						return commentInfo.getCreateDate();
					}
				});

		// Query 1
		DataStream<Top3Article> top3Articles = stream
				.map(CommentInfo::getArticleID)
				.keyBy(s -> s)
				.timeWindow(Time.hours(1))
				.aggregate(new CounterAggregateFunction<>())
				.timeWindowAll(Time.hours(1))
				.aggregate(new TopAggregateFunction<>(3, new SerializableCallback<Comparator<Tuple2<String, Long>>>() {
					@Override
					public Comparator<Tuple2<String, Long>> call() {
						return Comparator.comparingLong(value -> value.f1);
					}
				}))
				.map(item -> new Top3Article());


		//stream.print();
		top3Articles.print();

		// execute program
		env.execute("JASMINE Stream");
	}
}
