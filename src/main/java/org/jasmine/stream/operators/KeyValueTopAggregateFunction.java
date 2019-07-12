package org.jasmine.stream.operators;

import org.jasmine.stream.utils.BoundedPriorityQueue;
import org.jasmine.stream.utils.KeyValue;

import java.util.Comparator;

public class KeyValueTopAggregateFunction<K, V extends Comparable<V>> extends TopAggregateFunction<KeyValue<K, V>> {
    private int maxItems;

    public KeyValueTopAggregateFunction(int maxItems) {
        this.maxItems = maxItems;
    }

    @Override
    public BoundedPriorityQueue<KeyValue<K, V>> createAccumulator() {
        return new BoundedPriorityQueue<>(this.maxItems, Comparator.comparing(value -> value.f1));
    }

    public static class Merge<K, V extends Comparable<V>> extends TopAggregateFunction.Merge<KeyValue<K, V>> {

        private int maxItems;

        public Merge(int maxItems) {
            this.maxItems = maxItems;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<KeyValue<K, V>> getElementClass() {
            return (Class<KeyValue<K, V>>) (Class<?>) KeyValue.class;
        }

        @Override
        public BoundedPriorityQueue<KeyValue<K, V>> createAccumulator() {
            return new BoundedPriorityQueue<>(this.maxItems, Comparator.comparing(value -> value.f1));
        }
    }

}
