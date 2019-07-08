package org.jasmine.stream.queries;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.operators.*;

public class TopUserRatingsQuery {
    @SuppressWarnings("Duplicates")
    public static DataStream<TopUserRatings> run(DataStream<CommentInfo> inputStream, Time window) {
        DataStream<Tuple2<Tuple2<Long, String>, Double>> likesCount = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple2<>(new Tuple2<>(item.getUserID(), item.getUserDisplayName()), item.getRecommendations() * (item.isEditorsSelection() ? 1.1 : 1))).returns(Types.TUPLE(Types.TUPLE(Types.LONG, Types.STRING), Types.DOUBLE))
                .keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<String, Long>> indirectCommentsCount = inputStream
                .filter(item -> item.getDepth() > 1)
                .map(CommentInfo::getParentUserDisplayName)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CounterAggregateFunction<>());

        return likesCount.coGroup(indirectCommentsCount)
                .where(item -> item.f0.f1).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .apply(new LikesAndCommentsCoGroupFunction())
                .windowAll(TumblingEventTimeWindows.of(window))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));
    }

    @SuppressWarnings("Duplicates")
    public static Tuple3<DataStream<TopUserRatings>, DataStream<TopUserRatings>, DataStream<TopUserRatings>> runAll(DataStream<CommentInfo> inputStream) {
        Time window24h = Time.hours(24);
        Time window7d = Time.days(7);
        Time window1M = Time.days(30);

        DataStream<Tuple2<Tuple2<Long, String>, Double>> likesCountWindow24hStream = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple2<>(new Tuple2<>(item.getUserID(), item.getUserDisplayName()), item.getRecommendations() * (item.isEditorsSelection() ? 1.1 : 1))).returns(Types.TUPLE(Types.TUPLE(Types.LONG, Types.STRING), Types.DOUBLE))
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window24h))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Tuple2<Long, String>, Double>> likesCountWindow7dStream = likesCountWindow24hStream
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window7d))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Tuple2<Long, String>, Double>> likesCountWindow1MStream = likesCountWindow7dStream
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window1M))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<String, Long>> indirectCommentsCountWindow24hStream = inputStream
                .filter(item -> item.getDepth() > 1)
                .map(CommentInfo::getParentUserDisplayName)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new CounterAggregateFunction<>());

        DataStream<Tuple2<String, Long>> indirectCommentsCountWindow7dStream = indirectCommentsCountWindow24hStream
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(window7d))
                .reduce(new CounterReduceFunction<>());

        DataStream<Tuple2<String, Long>> indirectCommentsCountWindow1MStream = indirectCommentsCountWindow7dStream
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(window1M))
                .reduce(new CounterReduceFunction<>());

        DataStream<TopUserRatings> window24hStream = likesCountWindow24hStream.coGroup(indirectCommentsCountWindow24hStream)
                .where(item -> item.f0.f1).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window24h))
                .apply(new LikesAndCommentsCoGroupFunction())
                .windowAll(TumblingEventTimeWindows.of(window24h))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        DataStream<TopUserRatings> window7dStream = likesCountWindow7dStream.coGroup(indirectCommentsCountWindow7dStream)
                .where(item -> item.f0.f1).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window7d))
                .apply(new LikesAndCommentsCoGroupFunction())
                .windowAll(TumblingEventTimeWindows.of(window7d))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        DataStream<TopUserRatings> window1MStream = likesCountWindow1MStream.coGroup(indirectCommentsCountWindow1MStream)
                .where(item -> item.f0.f1).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window1M))
                .apply(new LikesAndCommentsCoGroupFunction())
                .windowAll(TumblingEventTimeWindows.of(window1M))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        return new Tuple3<>(window24hStream, window7dStream, window1MStream);
    }

    private static class LikesAndCommentsCoGroupFunction implements CoGroupFunction<Tuple2<Tuple2<Long, String>, Double>, Tuple2<String, Long>, Tuple2<Long, Double>> {
        @Override
        public void coGroup(Iterable<Tuple2<Tuple2<Long, String>, Double>> iterable, Iterable<Tuple2<String, Long>> iterable1, Collector<Tuple2<Long, Double>> collector) {
            Tuple2<Tuple2<Long, String>, Double> likesCountTuple = iterable.iterator().hasNext() ? iterable.iterator().next() : null;
            Tuple2<String, Long> indirectCommentsCountTuple = iterable1.iterator().hasNext() ? iterable1.iterator().next() : null;
            Long userID = likesCountTuple != null ? likesCountTuple.f0.f0 : 0L;
            double likesCount = likesCountTuple != null ? likesCountTuple.f1 : 0d;
            double indirectCommentsCount = indirectCommentsCountTuple != null ? indirectCommentsCountTuple.f1 : 0d;
            collector.collect(new Tuple2<>(userID, (.3 * likesCount + .7 * indirectCommentsCount)));
        }
    }
}
