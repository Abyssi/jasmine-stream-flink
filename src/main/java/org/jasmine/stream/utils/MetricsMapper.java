package org.jasmine.stream.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.jasmine.stream.config.FlinkConfiguration;

import java.io.IOException;
import java.util.List;

public class MetricsMapper<T> extends RichMapFunction<T, T> {

    private Meter meter;
    private Counter counter;

    public static <S> SingleOutputStreamOperator<S> wrap(SingleOutputStreamOperator<S> singleOutputStreamOperator) {
        try {
            List names = new ObjectMapper().readValue(FlinkConfiguration.getParameters().get("flink.metric.enabled.names", "[]"), List.class);
            if (names.contains(singleOutputStreamOperator.getName()))
                return singleOutputStreamOperator.map(new MetricsMapper<>()).name("MetricsMapper");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return singleOutputStreamOperator;
    }

    @Override
    public void open(Configuration config) {
        this.counter = getRuntimeContext()
                .getMetricGroup()
                .counter("counter");
        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("throughput", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public T map(T o) {
        this.counter.inc();
        this.meter.markEvent();
        return o;
    }
}
