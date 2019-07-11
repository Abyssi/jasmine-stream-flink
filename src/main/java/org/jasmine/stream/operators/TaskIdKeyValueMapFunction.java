package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class TaskIdKeyValueMapFunction<I> extends RichMapFunction<I, Tuple2<Integer, I>> {
    @Override
    public Tuple2<Integer, I> map(I i) throws Exception {
        return new Tuple2<>(this.getRuntimeContext().getIndexOfThisSubtask(), i);
    }
}
