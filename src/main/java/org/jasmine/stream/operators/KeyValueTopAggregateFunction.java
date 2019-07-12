package org.jasmine.stream.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.jasmine.stream.utils.BoundedPriorityQueue;

import java.util.Comparator;

public class KeyValueTopAggregateFunction<K, V extends Comparable<V>> extends TopAggregateFunction<Tuple2<K, V>> {
    private int maxItems;

    public KeyValueTopAggregateFunction(int maxItems) {
        this.maxItems = maxItems;
    }

    @Override
    public BoundedPriorityQueue<Tuple2<K, V>> createAccumulator() {
        return new BoundedPriorityQueue<>(this.maxItems, Comparator.comparing(value -> value.f1));
    }

    public static class Merge<K, V extends Comparable<V>> extends TopAggregateFunction.Merge<Tuple2<K, V>> {

        private int maxItems;

        public Merge(int maxItems) {
            this.maxItems = maxItems;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<Tuple2<K, V>> getElementClass() {
            return (Class<Tuple2<K, V>>) (Class<?>) Tuple2.class;
        }

        @Override
        public BoundedPriorityQueue<Tuple2<K, V>> createAccumulator() {
            return new BoundedPriorityQueue<>(this.maxItems, Comparator.comparing(value -> value.f1));
        }
    }

}
