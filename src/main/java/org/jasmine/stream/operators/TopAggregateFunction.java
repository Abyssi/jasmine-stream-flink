package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.jasmine.stream.utils.BoundedPriorityQueue;

abstract public class TopAggregateFunction<E> implements AggregateFunction<E, BoundedPriorityQueue<E>, BoundedPriorityQueue<E>> {

    @Override
    public BoundedPriorityQueue<E> add(E e, BoundedPriorityQueue<E> boundedPriorityQueue) {
        boundedPriorityQueue.add(e);
        return boundedPriorityQueue;
    }

    @Override
    public BoundedPriorityQueue<E> getResult(BoundedPriorityQueue<E> boundedPriorityQueue) {
        return boundedPriorityQueue;
    }

    @Override
    public BoundedPriorityQueue<E> merge(BoundedPriorityQueue<E> boundedPriorityQueue, BoundedPriorityQueue<E> acc1) {
        acc1.merge(boundedPriorityQueue);
        return acc1;
    }
}
