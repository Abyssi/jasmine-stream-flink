package org.jasmine.stream.utils;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jasmine.stream.config.FlinkConfiguration;

public class JNStreamExecutionEnvironment {

    public static StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //configure environment
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.setParallelism(FlinkConfiguration.getParameters().getInt("flink.parallelism", 4));
        if (!FlinkConfiguration.getParameters().getBoolean("flink.operation-chaining.enabled", false))
            environment.disableOperatorChaining();

        if (FlinkConfiguration.getParameters().getInt("flink.checkpoint.millis", 0) != 0) {
            environment.enableCheckpointing(FlinkConfiguration.getParameters().getInt("flink.checkpoint.millis", 60000));
            environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            environment.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        }
        if (FlinkConfiguration.getParameters().getInt("flink.latency.millis", 0) != 0)
            environment.getConfig().setLatencyTrackingInterval(FlinkConfiguration.getParameters().getInt("flink.latency.millis", 5));

        if (FlinkConfiguration.getParameters().getInt("flink.memory-state-size", 0) != 0)
            environment.setStateBackend((StateBackend) new MemoryStateBackend(FlinkConfiguration.getParameters().getInt("flink.memory-state-size", 5242880)));

        if (FlinkConfiguration.getParameters().getBoolean("flink.snapshot-compression.enabled", false))
            environment.getConfig().setUseSnapshotCompression(true);

        return environment;
    }
}
