package org.jasmine.stream.queries;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.operators.*;
import org.jasmine.stream.utils.KeyValue;

public class TopUserRatingsQuery {
    @SuppressWarnings("Duplicates")
    public static DataStream<TopUserRatings> run(FlinkJedisPoolConfig conf, DataStream<CommentInfo> inputStream, Time window) {
        inputStream.addSink(new RedisSink<>(conf, new RedisKeyValueMapper<>(item -> String.valueOf(item.getCommentID()), item -> String.valueOf(item.getUserID()))));

        DataStream<Tuple2<Long, Double>> likesCount = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple2<>(item.getUserID(), item.getRecommendations() * (item.isEditorsSelection() ? 1.1 : 1))).returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .keyBy(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Long, Long>> indirectCommentsCount = inputStream
                .filter(item -> item.getDepth() > 1)
                .map(CommentInfo::getInReplyTo)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new CounterAggregateFunction<>())
                .flatMap(new RedisKeyValueFlatMapFunction<>(conf, item -> item.f0.toString(), item -> new Tuple2<>(Long.valueOf(item.f1), item.f0.f1))).returns(Types.TUPLE(Types.LONG, Types.LONG));

        return likesCount.coGroup(indirectCommentsCount)
                .where(item -> item.f0).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .apply(new LikesAndCommentsCoGroupFunction())
                .map(KeyValue::new).returns(TypeInformation.of(new TypeHint<KeyValue<Long, Double>>() {
                }))
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window))
                .aggregate(new KeyValueTopAggregateFunction<>(10))
                .windowAll(TumblingEventTimeWindows.of(window))
                .aggregate(new LongDoubleKeyValueTopAggregateFunction(10), new TimestampEnrichProcessAllWindowFunction<>()).setParallelism(1)
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));
    }

    @SuppressWarnings("Duplicates")
    public static Tuple3<DataStream<TopUserRatings>, DataStream<TopUserRatings>, DataStream<TopUserRatings>> runAll(FlinkJedisPoolConfig conf, DataStream<CommentInfo> inputStream) {
        Time window24h = Time.hours(24);
        Time window7d = Time.days(7);
        Time window1M = Time.days(30);

        inputStream.addSink(new RedisSink<>(conf, new RedisKeyValueMapper<>(item -> String.valueOf(item.getCommentID()), item -> String.valueOf(item.getUserID()))));

        DataStream<Tuple2<Long, Double>> likesCountWindow24hStream = inputStream
                .map(item -> new Tuple2<>(item.getUserID(), item.getRecommendations() * (item.isEditorsSelection() ? 1.1 : 1))).returns(Types.TUPLE(Types.LONG, Types.DOUBLE))
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window24h))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Long, Double>> likesCountWindow7dStream = likesCountWindow24hStream
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window7d))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Long, Double>> likesCountWindow1MStream = likesCountWindow7dStream
                .keyBy(new KeyValueKeySelector<>())
                .window(TumblingEventTimeWindows.of(window1M))
                .reduce(new DecimalCounterReduceFunction<>());

        DataStream<Tuple2<Long, Long>> indirectCommentsCountWindow24hStream = inputStream
                .filter(item -> item.getDepth() > 1)
                .map(CommentInfo::getInReplyTo)
                .keyBy(s -> s)
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new CounterAggregateFunction<>())
                .flatMap(new RedisKeyValueFlatMapFunction<>(conf, item -> item.f0.toString(), item -> new Tuple2<>(Long.valueOf(item.f1), item.f0.f1))).returns(Types.TUPLE(Types.LONG, Types.LONG));

        DataStream<Tuple2<Long, Long>> indirectCommentsCountWindow7dStream = indirectCommentsCountWindow24hStream
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(window7d))
                .reduce(new CounterReduceFunction<>());

        DataStream<Tuple2<Long, Long>> indirectCommentsCountWindow1MStream = indirectCommentsCountWindow7dStream
                .keyBy(s -> s.f0)
                .window(TumblingEventTimeWindows.of(window1M))
                .reduce(new CounterReduceFunction<>());

        DataStream<TopUserRatings> window24hStream = likesCountWindow24hStream.coGroup(indirectCommentsCountWindow24hStream)
                .where(item -> item.f0).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window24h))
                .apply(new LikesAndCommentsCoGroupFunction())
                .map(KeyValue::new).returns(TypeInformation.of(new TypeHint<KeyValue<Long, Double>>() {
                }))
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window24h))
                .aggregate(new KeyValueTopAggregateFunction<>(10))
                .windowAll(TumblingEventTimeWindows.of(window24h))
                .aggregate(new LongDoubleKeyValueTopAggregateFunction(10), new TimestampEnrichProcessAllWindowFunction<>()).setParallelism(1)
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        DataStream<TopUserRatings> window7dStream = likesCountWindow7dStream.coGroup(indirectCommentsCountWindow7dStream)
                .where(item -> item.f0).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window7d))
                .apply(new LikesAndCommentsCoGroupFunction())
                .map(KeyValue::new).returns(TypeInformation.of(new TypeHint<KeyValue<Long, Double>>() {
                }))
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window7d))
                .aggregate(new KeyValueTopAggregateFunction<>(10))
                .windowAll(TumblingEventTimeWindows.of(window7d))
                .aggregate(new LongDoubleKeyValueTopAggregateFunction(10), new TimestampEnrichProcessAllWindowFunction<>()).setParallelism(1)
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        DataStream<TopUserRatings> window1MStream = likesCountWindow1MStream.coGroup(indirectCommentsCountWindow1MStream)
                .where(item -> item.f0).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window1M))
                .apply(new LikesAndCommentsCoGroupFunction())
                .map(KeyValue::new).returns(TypeInformation.of(new TypeHint<KeyValue<Long, Double>>() {
                }))
                .map(new TaskIdKeyValueMapFunction<>())
                .keyBy(new IdentifiedIdKeySelector<>())
                .window(TumblingEventTimeWindows.of(window1M))
                .aggregate(new KeyValueTopAggregateFunction<>(10))
                .windowAll(TumblingEventTimeWindows.of(window1M))
                .aggregate(new LongDoubleKeyValueTopAggregateFunction(10), new TimestampEnrichProcessAllWindowFunction<>()).setParallelism(1)
                .map(item -> new TopUserRatings(item.getTimestamp(), item.getElement()));

        return new Tuple3<>(window24hStream, window7dStream, window1MStream);
    }

    private static class LikesAndCommentsCoGroupFunction implements CoGroupFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
        @Override
        public void coGroup(Iterable<Tuple2<Long, Double>> iterable, Iterable<Tuple2<Long, Long>> iterable1, Collector<Tuple2<Long, Double>> collector) {
            Tuple2<Long, Double> likesCountTuple = iterable.iterator().hasNext() ? iterable.iterator().next() : null;
            Tuple2<Long, Long> indirectCommentsCountTuple = iterable1.iterator().hasNext() ? iterable1.iterator().next() : null;
            Long userID = likesCountTuple != null ? likesCountTuple.f0 : indirectCommentsCountTuple != null ? indirectCommentsCountTuple.f0 : 0L;
            double likesCount = likesCountTuple != null ? likesCountTuple.f1 : 0d;
            double indirectCommentsCount = indirectCommentsCountTuple != null ? indirectCommentsCountTuple.f1 : 0d;
            collector.collect(new Tuple2<>(userID, (.3 * likesCount + .7 * indirectCommentsCount)));
        }
    }

    private static class LongDoubleKeyValueTopAggregateFunction extends KeyValueTopAggregateFunction.Merge<Long, Double> {
        public LongDoubleKeyValueTopAggregateFunction(int maxItems) {
            super(maxItems);
        }
    }
}
