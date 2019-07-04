package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.jasmine.stream.utils.BoundedPriorityQueue;

abstract public class TopAggregateFunction<E> implements AggregateFunction<E, BoundedPriorityQueue<E>, E[]> {

    @Override
    public BoundedPriorityQueue<E> add(E e, BoundedPriorityQueue<E> boundedPriorityQueue) {
        boundedPriorityQueue.add(e);
        return boundedPriorityQueue;
    }

    @Override
    public E[] getResult(BoundedPriorityQueue<E> boundedPriorityQueue) {
        return boundedPriorityQueue.toSortedArray(this.getElementClass());
    }

    @Override
    public BoundedPriorityQueue<E> merge(BoundedPriorityQueue<E> boundedPriorityQueue, BoundedPriorityQueue<E> acc1) {
        acc1.merge(boundedPriorityQueue);
        return acc1;
    }

    public abstract Class<E> getElementClass();
}
