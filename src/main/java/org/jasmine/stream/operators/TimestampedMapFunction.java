package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.jasmine.stream.utils.Timestamped;

public class TimestampedMapFunction<I, O> implements MapFunction<Timestamped<I>, Timestamped<O>> {

    private MapFunction<I, O> mapFunction;

    public TimestampedMapFunction(MapFunction<I, O> mapFunction) {
        this.mapFunction = mapFunction;
    }

    @Override
    public Timestamped<O> map(Timestamped<I> iTimestamped) throws Exception {
        return new Timestamped<>(this.mapFunction.map(iTimestamped.getElement()), iTimestamped.getTimestamp());
    }
}
