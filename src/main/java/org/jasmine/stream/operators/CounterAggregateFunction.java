package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CounterAggregateFunction<E> implements AggregateFunction<E, Tuple2<E, Long>, Tuple2<E, Long>> {
    @Override
    public Tuple2<E, Long> createAccumulator() {
        return new Tuple2<>(null, 0L);
    }

    @Override
    public Tuple2<E, Long> add(E e, Tuple2<E, Long> elemLongTuple2) {
        elemLongTuple2.f0 = e;
        elemLongTuple2.f1 += 1;
        return elemLongTuple2;
    }

    @Override
    public Tuple2<E, Long> getResult(Tuple2<E, Long> elemLongTuple2) {
        return elemLongTuple2;
    }

    @Override
    public Tuple2<E, Long> merge(Tuple2<E, Long> elemLongTuple2, Tuple2<E, Long> acc1) {
        return new Tuple2<>(acc1.f0, acc1.f1 + elemLongTuple2.f1);
    }
}
