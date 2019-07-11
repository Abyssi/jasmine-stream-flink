package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyValueValueMapFunction<K, V> implements MapFunction<Tuple2<K, V>, V> {
    @Override
    public V map(Tuple2<K, V> kvTuple2) throws Exception {
        return kvTuple2.f1;
    }
}
