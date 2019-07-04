package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;

public class PeekMapFunction<E> implements MapFunction<E, E> {
    @Override
    public E map(E e) {
        System.out.println("[Peek] - " + e);
        return e;
    }
}
