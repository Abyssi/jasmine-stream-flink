package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.jasmine.stream.utils.Timestamped;

public class TimestampedAggregateFunction<E> implements AggregateFunction<Timestamped<E>, Timestamped<E>, Timestamped<E>> {
    @Override
    public Timestamped<E> createAccumulator() {
        return null;
    }

    @Override
    public Timestamped<E> add(Timestamped<E> eTimestamped, Timestamped<E> eTimestamped2) {
        return null;
    }

    @Override
    public Timestamped<E> getResult(Timestamped<E> eTimestamped) {
        return null;
    }

    @Override
    public Timestamped<E> merge(Timestamped<E> eTimestamped, Timestamped<E> acc1) {
        return null;
    }
}
