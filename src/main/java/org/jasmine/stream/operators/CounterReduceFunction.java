package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class CounterReduceFunction<E> implements ReduceFunction<Tuple2<E, Long>> {
    @Override
    public Tuple2<E, Long> reduce(Tuple2<E, Long> eLongTuple2, Tuple2<E, Long> t1) {
        t1.f1 += eLongTuple2.f1;
        return t1;
    }
}
