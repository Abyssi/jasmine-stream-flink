package org.jasmine.stream.operators;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jasmine.stream.utils.Timestamped;

public class TimestampEnrichProcessAllWindowFunction<E> extends ProcessAllWindowFunction<E, Timestamped<E>, TimeWindow> {
    @Override
    public void process(Context context, Iterable<E> iterable, Collector<Timestamped<E>> out) {
        out.collect(new Timestamped<>(iterable.iterator().next(), context.window().getStart()));
    }
}
