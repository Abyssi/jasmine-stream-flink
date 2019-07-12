package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.jasmine.stream.utils.BoundedPriorityQueue;
import org.jasmine.stream.utils.Identified;

abstract public class TopAggregateFunction<E> implements AggregateFunction<Identified.ByInteger<E>, BoundedPriorityQueue<E>, BoundedPriorityQueue<E>> {

    @Override
    public BoundedPriorityQueue<E> add(Identified.ByInteger<E> e, BoundedPriorityQueue<E> boundedPriorityQueue) {
        boundedPriorityQueue.add(e.getElement());
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

    public abstract static class Merge<E> implements AggregateFunction<BoundedPriorityQueue<E>, BoundedPriorityQueue<E>, E[]> {

        @Override
        public BoundedPriorityQueue<E> add(BoundedPriorityQueue<E> es, BoundedPriorityQueue<E> boundedPriorityQueue) {
            return boundedPriorityQueue.merge(es);
        }

        @Override
        public E[] getResult(BoundedPriorityQueue<E> boundedPriorityQueue) {
            return boundedPriorityQueue.toSortedArray(this.getElementClass());
        }

        @Override
        public BoundedPriorityQueue<E> merge(BoundedPriorityQueue<E> boundedPriorityQueue, BoundedPriorityQueue<E> acc1) {
            return acc1.merge(boundedPriorityQueue);
        }

        public abstract Class<E> getElementClass();
    }

}
