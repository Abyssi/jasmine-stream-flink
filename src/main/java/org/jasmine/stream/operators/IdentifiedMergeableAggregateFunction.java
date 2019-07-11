package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.jasmine.stream.utils.Identified;
import org.jasmine.stream.utils.Mergeable;

public class IdentifiedMergeableAggregateFunction<E extends Mergeable<E>> implements AggregateFunction<Identified.ByInteger<E>, E, E> {
    @Override
    public E createAccumulator() {
        return null;
    }

    @Override
    public E add(Identified.ByInteger<E> eByInteger, E e) {
        if (e == null) return eByInteger.getElement();
        return e.merge(eByInteger.getElement());
    }

    @Override
    public E getResult(E e) {
        return e;
    }

    @Override
    public E merge(E e, E acc1) {
        return acc1.merge(e);
    }
}
