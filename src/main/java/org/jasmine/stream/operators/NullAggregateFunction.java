package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;

public class NullAggregateFunction<IN, ACC, OUT> implements AggregateFunction<IN, ACC, OUT> {
    @Override
    public ACC createAccumulator() {
        return null;
    }

    @Override
    public ACC add(IN in, ACC acc) {
        return null;
    }

    @Override
    public OUT getResult(ACC acc) {
        return null;
    }

    @Override
    public ACC merge(ACC acc, ACC acc1) {
        return null;
    }
}
