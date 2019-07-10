package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisKeyValueMapper<E> implements RedisMapper<E> {

    private MapFunction<E, String> keyFunction;
    private MapFunction<E, String> valueFunction;

    public RedisKeyValueMapper(MapFunction<E, String> keyFunction, MapFunction<E, String> valueFunction) {
        this.keyFunction = keyFunction;
        this.valueFunction = valueFunction;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(E e) {
        try {
            return this.keyFunction.map(e);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return "";
    }

    @Override
    public String getValueFromData(E e) {
        try {
            return this.valueFunction.map(e);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        return "";
    }
}
