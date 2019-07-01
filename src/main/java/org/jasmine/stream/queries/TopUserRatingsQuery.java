package org.jasmine.stream.queries;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.TopUserRatings;
import org.jasmine.stream.operators.CounterAggregateFunction;
import org.jasmine.stream.operators.DecimalCounterAggregateFunction;
import org.jasmine.stream.operators.TimestampEnrichProcessAllWindowFunction;
import org.jasmine.stream.operators.TopAggregateFunction;
import org.jasmine.stream.utils.BoundedPriorityQueue;

import java.util.Comparator;

public class TopUserRatingsQuery {
    public static DataStream<TopUserRatings> run(DataStream<CommentInfo> inputStream, Time window) {
        DataStream<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>> likesCount = inputStream
                .filter(item -> item.getDepth() == 1)
                .map(item -> new Tuple4<>(item.getUserID(), item.getRecommendations(), item.isEditorsSelection(), item.getUserDisplayName())).returns(Types.TUPLE(Types.LONG, Types.LONG, Types.BOOLEAN, Types.STRING))
                .keyBy(item -> item.f0)
                .timeWindow(window)
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
                .timeWindow(window)
                .aggregate(new CounterAggregateFunction<>());

        double wa = 0.3;
        double wb = 0.7;
        return likesCount.join(indirectCommentsCount)
                .where(item -> item.f0.f3).equalTo(item -> item.f0)
                .window(TumblingEventTimeWindows.of(window))
                .apply(new JoinFunction<Tuple2<Tuple4<Long, Long, Boolean, String>, Double>, Tuple2<String, Long>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> join(Tuple2<Tuple4<Long, Long, Boolean, String>, Double> tuple4DoubleTuple2, Tuple2<String, Long> stringLongTuple2) throws Exception {
                        return new Tuple2<>(tuple4DoubleTuple2.f0.f0, wa * tuple4DoubleTuple2.f1 + wb * stringLongTuple2.f1);
                    }
                })
                .timeWindowAll(window)
                .aggregate(new TopAggregateFunction<Tuple2<Long, Double>>() {
                    @Override
                    public BoundedPriorityQueue<Tuple2<Long, Double>> createAccumulator() {
                        return new BoundedPriorityQueue<>(10, Comparator.comparingDouble(value -> value.f1));
                    }
                }, new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> {
                    Tuple2<Long, Double>[] array = item.getElement().toArray();
                    return new TopUserRatings(item.getTimestamp(), array[0].f0, array[0].f1, array[1].f0, array[1].f1, array[2].f0, array[2].f1, array[3].f0, array[3].f1, array[4].f0, array[4].f1, array[5].f0, array[5].f1, array[6].f0, array[6].f1, array[7].f0, array[7].f1, array[8].f0, array[8].f1, array[9].f0, array[9].f1);
                });
    }
}
