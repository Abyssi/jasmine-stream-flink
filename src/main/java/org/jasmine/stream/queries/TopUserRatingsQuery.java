package org.jasmine.stream.queries;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.operators.*;

public class TopUserRatingsQuery {
    @SuppressWarnings("Duplicates")
    public static DataStream<TopUserRatings> run(DataStream<CommentInfo> inputStream, Time window) {
        DataStream<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>> likesCount = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple4<>(item.getUserID(), item.getRecommendations(), item.isEditorsSelection(), item.getUserDisplayName())).returns(Types.TUPLE(Types.LONG, Types.LONG, Types.BOOLEAN, Types.STRING))
                .keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CustomLikesCounterAggregateFunction());

        DataStream<Tuple2<String, Long>> indirectCommentsCount = inputStream
                .filter(item -> item.getDepth() > 1)
                .map(CommentInfo::getParentUserDisplayName)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CounterAggregateFunction<>());

        return likesCount.join(indirectCommentsCount)
                .where(item -> item.f0.f3).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .apply(new LikesAndCommentsJoinFunction())
                .windowAll(TumblingEventTimeWindows.of(window))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));
    }

    @SuppressWarnings("Duplicates")
    public static Tuple3<DataStream<TopUserRatings>, DataStream<TopUserRatings>, DataStream<TopUserRatings>> runAll(DataStream<CommentInfo> inputStream) {
        Time window24h = Time.hours(24);
        Time window7d = Time.days(7);
        Time window1M = Time.days(30);

        DataStream<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>> likesCountWindow24hStream = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple4<>(item.getUserID(), item.getRecommendations(), item.isEditorsSelection(), item.getUserDisplayName())).returns(Types.TUPLE(Types.LONG, Types.LONG, Types.BOOLEAN, Types.STRING))
                .keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new CustomLikesCounterAggregateFunction());

        DataStream<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>> likesCountWindow7dStream = likesCountWindow24hStream
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window7d))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>> likesCountWindow1MStream = likesCountWindow7dStream
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

        DataStream<TopUserRatings> window24hStream = likesCountWindow24hStream.join(indirectCommentsCountWindow24hStream)
                .where(item -> item.f0.f3).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window24h))
                .apply(new LikesAndCommentsJoinFunction())
                .windowAll(TumblingEventTimeWindows.of(window24h))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        DataStream<TopUserRatings> window7dStream = likesCountWindow7dStream.join(indirectCommentsCountWindow7dStream)
                .where(item -> item.f0.f3).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window7d))
                .apply(new LikesAndCommentsJoinFunction())
                .windowAll(TumblingEventTimeWindows.of(window7d))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        DataStream<TopUserRatings> window1MStream = likesCountWindow1MStream.join(indirectCommentsCountWindow1MStream)
                .where(item -> item.f0.f3).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window1M))
                .apply(new LikesAndCommentsJoinFunction())
                .windowAll(TumblingEventTimeWindows.of(window1M))
                .aggregate(new KeyValueTopAggregateFunction<>(10), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        return new Tuple3<>(window24hStream, window7dStream, window1MStream);
    }

    private static class CustomLikesCounterAggregateFunction extends DecimalCounterAggregateFunction<Tuple4<Long, Long, Boolean, String>> {
        @Override
        public Tuple2<Tuple4<Long, Long, Boolean, String>, Double> add(Tuple4<Long, Long, Boolean, String> e, Tuple2<Tuple4<Long, Long, Boolean, String>, Double> tuple4DoubleTuple2) {
            tuple4DoubleTuple2.f0 = e;
            tuple4DoubleTuple2.f1 += e.f1 * (e.f2 ? 1.1 : 1);
            return tuple4DoubleTuple2;
        }
    }

    private static class LikesAndCommentsJoinFunction implements JoinFunction<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>, Tuple2<String, Long>, Tuple2<Long, Double>> {
        @Override
        public Tuple2<Long, Double> join(Tuple2<Tuple4<Long, Long, Boolean, String>, Double> tuple4DoubleTuple2, Tuple2<String, Long> stringLongTuple2) {
            return new Tuple2<>(tuple4DoubleTuple2.f0.f0, (.3 * tuple4DoubleTuple2.f1 + .7 * stringLongTuple2.f1));
        }
    }


}
