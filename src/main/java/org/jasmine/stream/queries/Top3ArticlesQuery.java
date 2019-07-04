package org.jasmine.stream.queries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.Top3Article;
import org.jasmine.stream.operators.CounterAggregateFunction;
import org.jasmine.stream.operators.PeekMapFunction;
import org.jasmine.stream.operators.TimestampEnrichProcessAllWindowFunction;
import org.jasmine.stream.operators.TopAggregateFunction;
import org.jasmine.stream.utils.BoundedPriorityQueue;

import java.util.Comparator;

public class Top3ArticlesQuery {
    public static DataStream<Top3Article> run(DataStream<CommentInfo> inputStream, Time window) {
        return inputStream
                .map(CommentInfo::getArticleID)
                .keyBy(s -> s)
                .timeWindow(window)
                .aggregate(new CounterAggregateFunction<>())
                .map(new PeekMapFunction<>())
                .timeWindowAll(window)
                .aggregate(new TopAggregateFunction<Tuple2<String, Long>>() {
                    @Override
                    public BoundedPriorityQueue<Tuple2<String, Long>> createAccumulator() {
                        return new BoundedPriorityQueue<>(3, Comparator.comparingLong(value -> value.f1));
                    }
                }, new TimestampEnrichProcessAllWindowFunction<>())
                .map(new PeekMapFunction<>())
                .map(item -> {
                    return null;
                    //Tuple2<String, Long>[] array = item.getElement().toSortedArray();
                    //return new Top3Article(item.getTimestamp(), array[0].f0, array[0].f1, array[1].f0, array[1].f1, array[2].f0, array[2].f1);
                });
    }
}
