package org.jasmine.stream.utils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.jasmine.stream.config.FlinkConfiguration;

public class LatencyTracker {

    private DataStream<Long> startStream;
    private DataStream<Long> endStream;

    public LatencyTracker() {
    }

    public <T1, T2> LatencyTracker(DataStream<T1> startTrackStream, DataStream<T2> endTrackStream) {
        this.trackStart(startTrackStream);
        this.trackEnd(endTrackStream);
    }

    public <T> DataStream<T> trackStart(DataStream<T> stream) {
        this.startStream = FlinkConfiguration.getParameters().getBoolean("latency-enabled") ? stream.flatMap(new LatencyStartFlatMapFunction<>()).setParallelism(1).name("LatencyTrackerStart") : stream.getExecutionEnvironment().fromElements(0L);
        return stream;
    }

    public <T> DataStream<T> trackEnd(DataStream<T> stream) {
        this.endStream = FlinkConfiguration.getParameters().getBoolean("latency-enabled") ? stream.connect(this.startStream).flatMap(new LatencyEndFlatMapFunction<>()).setParallelism(1).name("LatencyTrackerEnd") : stream.getExecutionEnvironment().fromElements(0L);
        return stream;
    }

    public DataStream<Long> getEndStream() {
        return endStream;
    }

    public void setEndStream(DataStream<Long> endStream) {
        this.endStream = endStream;
    }

    public static class LatencyStartFlatMapFunction<I> implements FlatMapFunction<I, Long> {
        private int index = 0;

        @Override
        public void flatMap(I i, Collector<Long> collector) throws Exception {
            if (index == 0) {
                index++;
                collector.collect(System.nanoTime());
            }
        }
    }

    public static class LatencyEndFlatMapFunction<I> implements CoFlatMapFunction<I, Long, Long> {
        private long lastTimestamp = 0;
        private int index = 0;

        @Override
        public void flatMap1(I i, Collector<Long> collector) throws Exception {
            if (index == 0) {
                index++;
                collector.collect(System.nanoTime() - this.lastTimestamp);
            }
        }

        @Override
        public void flatMap2(Long aLong, Collector<Long> collector) throws Exception {
            this.lastTimestamp = aLong;
        }
    }
}

