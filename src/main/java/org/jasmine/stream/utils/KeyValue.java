package org.jasmine.stream.utils;

import org.apache.flink.api.java.tuple.Tuple2;

public class KeyValue<K, V extends Comparable<V>> extends Tuple2<K, V> implements Comparable<KeyValue<K, V>> {

    public KeyValue(Tuple2<K, V> tuple2) {
        this.f0 = tuple2.f0;
        this.f1 = tuple2.f1;
    }

    @Override
    public int compareTo(KeyValue<K, V> other) {
        return other.f1.compareTo(this.f1);
    }
}
