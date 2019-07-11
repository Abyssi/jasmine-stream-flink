package org.jasmine.stream.queries;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.Top3Article;
import org.jasmine.stream.operators.*;
import org.jasmine.stream.utils.BoundedPriorityQueue;

public class TopArticlesQuery {
    @SuppressWarnings("Duplicates")
    public static DataStream<Top3Article> run(DataStream<CommentInfo> inputStream, Time window) {
        AggregateFunction<Tuple2<String, Long>, BoundedPriorityQueue<Tuple2<String, Long>>, BoundedPriorityQueue<Tuple2<String, Long>>> agg = new NullAggregateFunction<>();
        KeyValueAggregateFunction<Integer, Tuple2<String, Long>, BoundedPriorityQueue<Tuple2<String, Long>>, BoundedPriorityQueue<Tuple2<String, Long>>> abc = new KeyValueAggregateFunction<>(agg);
        System.out.println(abc.toString());

        return inputStream
                .map(CommentInfo::getArticleID)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CounterAggregateFunction<>())
                .map(new TaskIdKeyValueMapFunction<>()).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Tuple2<String, Long>>>(){}))
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(abc).returns(TypeInformation.of(new TypeHint<BoundedPriorityQueue<Tuple2<String, Long>>>(){}))
                .windowAll(TumblingEventTimeWindows.of(window))
                .reduce(new KeyValueTopAggregateFunction.Merge<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(new TimestampedMapFunction<>(new KeyValueTopAggregateFunction.MapToArray<>()))
                .map(item -> new Top3Article(item.getTimestamp(), item.getElement()));
    }

    @SuppressWarnings("Duplicates")
    public static Tuple3<DataStream<Top3Article>, DataStream<Top3Article>, DataStream<Top3Article>> runAll(DataStream<CommentInfo> inputStream) {
        Time window1h = Time.hours(1);
        Time window24h = Time.hours(24);
        Time window7d = Time.days(7);

        DataStream<Tuple2<String, Long>> intermediateWindow1hStream = inputStream
                .map(CommentInfo::getArticleID)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window1h))
                .aggregate(new CounterAggregateFunction<>());

        DataStream<Tuple2<String, Long>> intermediateWindow24hStream = intermediateWindow1hStream
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(window24h))
                .reduce(new CounterReduceFunction<>());

        DataStream<Tuple2<String, Long>> intermediateWindow7dStream = intermediateWindow24hStream
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(window7d))
                .reduce(new CounterReduceFunction<>());

        DataStream<Top3Article> window1hStream = intermediateWindow1hStream
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window1h))
                .aggregate(new KeyValueAggregateFunction<>(new KeyValueTopAggregateFunction<>(3))).returns(TypeInformation.of(new TypeHint<BoundedPriorityQueue<Tuple2<String, Long>>>(){}))
                .windowAll(TumblingEventTimeWindows.of(window1h))
                .reduce(new KeyValueTopAggregateFunction.Merge<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(new TimestampedMapFunction<>(new KeyValueTopAggregateFunction.MapToArray<>())).map(item -> new Top3Article(item.getTimestamp(), item.getElement()));

        DataStream<Top3Article> window24hStream = intermediateWindow24hStream
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new KeyValueAggregateFunction<>(new KeyValueTopAggregateFunction<>(3))).returns(TypeInformation.of(new TypeHint<BoundedPriorityQueue<Tuple2<String, Long>>>(){}))
                .windowAll(TumblingEventTimeWindows.of(window24h))
                .reduce(new KeyValueTopAggregateFunction.Merge<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(new TimestampedMapFunction<>(new KeyValueTopAggregateFunction.MapToArray<>())).map(item -> new Top3Article(item.getTimestamp(), item.getElement()));

        DataStream<Top3Article> window7dStream = intermediateWindow7dStream
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window7d))
                .aggregate(new KeyValueAggregateFunction<>(new KeyValueTopAggregateFunction<>(3))).returns(TypeInformation.of(new TypeHint<BoundedPriorityQueue<Tuple2<String, Long>>>(){}))
                .windowAll(TumblingEventTimeWindows.of(window7d))
                .reduce(new KeyValueTopAggregateFunction.Merge<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(new TimestampedMapFunction<>(new KeyValueTopAggregateFunction.MapToArray<>())).map(item -> new Top3Article(item.getTimestamp(), item.getElement()));

        return new Tuple3<>(window1hStream, window24hStream, window7dStream);
    }
}
