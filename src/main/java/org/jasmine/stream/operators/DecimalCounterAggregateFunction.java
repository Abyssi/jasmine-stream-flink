package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public abstract class DecimalCounterAggregateFunction<E> implements AggregateFunction<E, Tuple2<E, Double>, Tuple2<E, Double>> {
    @Override
    public Tuple2<E, Double> createAccumulator() {
        return new Tuple2<>(null, 0d);
    }

    @Override
    public Tuple2<E, Double> getResult(Tuple2<E, Double> elemDoubleTuple2) {
        return elemDoubleTuple2;
    }

    @Override
    public Tuple2<E, Double> merge(Tuple2<E, Double> elemDoubleTuple2, Tuple2<E, Double> acc1) {
        return new Tuple2<>(acc1.f0, acc1.f1 + elemDoubleTuple2.f1);
    }
}
