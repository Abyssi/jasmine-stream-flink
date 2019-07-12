package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class CollectorAggregateFunction<K, V> implements AggregateFunction<Tuple2<K, V>, HashMap<K, V>, HashMap<K, V>> {
    @Override
    public HashMap<K, V> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<K, V> add(Tuple2<K, V> kvTuple2, HashMap<K, V> kvHashMap) {
        kvHashMap.put(kvTuple2.f0, kvTuple2.f1);
        return kvHashMap;
    }

    @Override
    public HashMap<K, V> getResult(HashMap<K, V> kvHashMap) {
        return kvHashMap;
    }

    @Override
    public HashMap<K, V> merge(HashMap<K, V> kvHashMap, HashMap<K, V> acc1) {
        acc1.putAll(kvHashMap);
        return acc1;
    }
}
