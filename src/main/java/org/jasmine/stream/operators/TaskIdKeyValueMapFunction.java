package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.jasmine.stream.utils.Identified;

public class TaskIdKeyValueMapFunction<I> extends RichMapFunction<I, Identified.ByInteger<I>> {
    @Override
    public Identified.ByInteger<I> map(I i) throws Exception {
        return new Identified.ByInteger<>(this.getRuntimeContext().getIndexOfThisSubtask(), i);
    }
}
