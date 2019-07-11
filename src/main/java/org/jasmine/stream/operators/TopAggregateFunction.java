package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jasmine.stream.utils.BoundedPriorityQueue;
import org.jasmine.stream.utils.Identified;

import java.util.HashMap;

abstract public class TopAggregateFunction<E> implements AggregateFunction<Identified.ByInteger<E>, BoundedPriorityQueue<E>, BoundedPriorityQueue<E>> {

    @Override
    public BoundedPriorityQueue<E> add(Identified.ByInteger<E> e, BoundedPriorityQueue<E> boundedPriorityQueue) {
        boundedPriorityQueue.add(e.getElement());
        return boundedPriorityQueue;
    }

    @Override
    public BoundedPriorityQueue<E> getResult(BoundedPriorityQueue<E> boundedPriorityQueue) {
        System.out.println("FIRING: " + boundedPriorityQueue.clone());
        return boundedPriorityQueue.clone();
    }

    @Override
    public BoundedPriorityQueue<E> merge(BoundedPriorityQueue<E> boundedPriorityQueue, BoundedPriorityQueue<E> acc1) {
        acc1.merge(boundedPriorityQueue);
        return acc1;
    }

    public static class Merge<E> implements ReduceFunction<BoundedPriorityQueue<E>> {
        @Override
        public BoundedPriorityQueue<E> reduce(BoundedPriorityQueue<E> es, BoundedPriorityQueue<E> t1) throws Exception {
            t1.merge(es);
            return t1;
        }
    }

}
