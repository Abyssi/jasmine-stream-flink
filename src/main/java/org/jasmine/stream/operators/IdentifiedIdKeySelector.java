package org.jasmine.stream.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.jasmine.stream.utils.Identified;

public class IdentifiedIdKeySelector<V> implements KeySelector<Identified.ByInteger<V>, Integer> {
    @Override
    public Integer getKey(Identified.ByInteger<V> kvTuple2) {
        return kvTuple2.getId();
    }
}
