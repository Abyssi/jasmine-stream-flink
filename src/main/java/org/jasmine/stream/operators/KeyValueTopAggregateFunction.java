package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jasmine.stream.utils.BoundedPriorityQueue;

import java.util.Comparator;

public class KeyValueTopAggregateFunction<K, V extends Comparable<? super V>> extends TopAggregateFunction<Tuple2<K, V>> {
    private int maxItems;

    public KeyValueTopAggregateFunction(int maxItems) {
        this.maxItems = maxItems;
    }

    @Override
    public BoundedPriorityQueue<Tuple2<K, V>> createAccumulator() {
        return new BoundedPriorityQueue<>(this.maxItems, Comparator.comparing(value -> value.f1));
    }

    public static class MapToArray<K, V extends Comparable<? super V>> implements MapFunction<BoundedPriorityQueue<Tuple2<K, V>>, Tuple2<K, V>[]> {
        @Override
        @SuppressWarnings("unchecked")
        public Tuple2<K, V>[] map(BoundedPriorityQueue<Tuple2<K, V>> tuple2s) throws Exception {
            return tuple2s.toSortedArray((Class<Tuple2<K, V>>) (Class<?>) Tuple2.class);
        }
    }

}
