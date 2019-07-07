package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DecimalCounterReduceFunction<E> implements ReduceFunction<Tuple2<E, Double>> {
    @Override
    public Tuple2<E, Double> reduce(Tuple2<E, Double> eDoubleTuple2, Tuple2<E, Double> t1) {
        t1.f1 += eDoubleTuple2.f1;
        return t1;
    }
}
