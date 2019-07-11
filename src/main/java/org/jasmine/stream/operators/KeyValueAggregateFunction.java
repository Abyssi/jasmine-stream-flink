package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jasmine.stream.utils.BoundedPriorityQueue;

public class KeyValueAggregateFunction<K, I, A, O> implements AggregateFunction<Tuple2<K, I>, A, O> {

    private AggregateFunction<I, A, O> aggregateFunction;

    public KeyValueAggregateFunction(AggregateFunction<I, A, O> aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    public A createAccumulator() {
        return this.aggregateFunction.createAccumulator();
    }

    @Override
    public A add(Tuple2<K, I> kiTuple2, A a) {
        return this.aggregateFunction.add(kiTuple2.f1, a);
    }

    @Override
    public O getResult(A a) {
        return this.aggregateFunction.getResult(a);
    }

    @Override
    public A merge(A a, A acc1) {
        return this.aggregateFunction.merge(a, acc1);
    }

}
