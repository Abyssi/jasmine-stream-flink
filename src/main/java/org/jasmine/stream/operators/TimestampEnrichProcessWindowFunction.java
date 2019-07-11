package org.jasmine.stream.operators;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.jasmine.stream.utils.Timestamped;

public class TimestampEnrichProcessWindowFunction<E, K> extends ProcessWindowFunction<E, Timestamped<E>, K, TimeWindow> {
    @Override
    public void process(K k, Context context, Iterable<E> iterable, Collector<Timestamped<E>> collector) throws Exception {
        collector.collect(new Timestamped<>(iterable.iterator().next(), context.window().getStart()));
    }
}
