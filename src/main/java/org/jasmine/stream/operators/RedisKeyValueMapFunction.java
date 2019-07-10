package org.jasmine.stream.operators;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

public class RedisKeyValueMapFunction<I, O> extends RichMapFunction<I, O> {

    private MapFunction<I, String> keyFunction;
    private MapFunction<Tuple2<I, String>, O> mapFunction;

    private FlinkJedisPoolConfig flinkJedisPoolConfig;
    private JedisPool jedisPool;

    public RedisKeyValueMapFunction(FlinkJedisPoolConfig flinkJedisPoolConfig, MapFunction<I, String> keyFunction, MapFunction<Tuple2<I, String>, O> mapFunction) {
        Preconditions.checkNotNull(flinkJedisPoolConfig, "Redis connection pool config should not be null");
        this.flinkJedisPoolConfig = flinkJedisPoolConfig;

        this.keyFunction = keyFunction;
        this.mapFunction = mapFunction;
    }

    private static JedisPool build(FlinkJedisPoolConfig jedisPoolConfig) {
        Preconditions.checkNotNull(jedisPoolConfig, "Redis pool config should not be Null");
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(jedisPoolConfig.getMaxIdle());
        genericObjectPoolConfig.setMaxTotal(jedisPoolConfig.getMaxTotal());
        genericObjectPoolConfig.setMinIdle(jedisPoolConfig.getMinIdle());
        return new JedisPool(genericObjectPoolConfig, jedisPoolConfig.getHost(), jedisPoolConfig.getPort(), jedisPoolConfig.getConnectionTimeout(), jedisPoolConfig.getPassword(), jedisPoolConfig.getDatabase());
    }

    public void open(Configuration parameters) throws Exception {
        this.jedisPool = RedisKeyValueMapFunction.build(this.flinkJedisPoolConfig);
    }

    public void close() throws IOException {
        if (this.jedisPool != null)
            this.jedisPool.close();
    }

    @Override
    public O map(I element) throws Exception {
        return this.mapFunction.map(new Tuple2<>(element, this.getValueFromRedis(this.keyFunction.map(element))));
    }

    private String getValueFromRedis(String key) {
        return this.jedisPool.getResource().get(key);
    }
}
