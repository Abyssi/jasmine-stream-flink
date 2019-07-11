package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class KeyValueReduceFunction<K, V> implements ReduceFunction<Tuple2<K, V>> {

    private ReduceFunction<V> reduceFunction;

    public KeyValueReduceFunction(ReduceFunction<V> reduceFunction) {
        this.reduceFunction = reduceFunction;
    }
    @Override
    public Tuple2<K, V> reduce(Tuple2<K, V> kvTuple2, Tuple2<K, V> t1) throws Exception {
        t1.f1 = this.reduceFunction.reduce(kvTuple2.f1, t1.f1);
        return t1;
    }
}
