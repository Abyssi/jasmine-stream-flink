package org.jasmine.stream.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyValueKeySelector<K, V> implements KeySelector<Tuple2<K, V>, K> {
    @Override
    public K getKey(Tuple2<K, V> kvTuple2) {
        return kvTuple2.f0;
    }
}
