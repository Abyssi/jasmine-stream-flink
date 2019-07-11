package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jasmine.stream.utils.Identified;

import java.util.HashMap;

public class CollectorAggregateFunction<K, V> implements AggregateFunction<Identified.ByInteger<Tuple2<K, V>>, HashMap<K, V>, HashMap<K, V>> {
    @Override
    public HashMap<K, V> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<K, V> add(Identified.ByInteger<Tuple2<K, V>> kvTuple2, HashMap<K, V> kvHashMap) {
        kvHashMap.put(kvTuple2.getElement().f0, kvTuple2.getElement().f1);
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

    public static class Merge<K, V> implements ReduceFunction<HashMap<K, V>> {
        @Override
        public HashMap<K, V> reduce(HashMap<K, V> kvHashMap, HashMap<K, V> t1) throws Exception {
            t1.putAll(kvHashMap);
            return t1;
        }
    }
}
