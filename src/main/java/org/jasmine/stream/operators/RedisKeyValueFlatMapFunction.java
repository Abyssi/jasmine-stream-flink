package org.jasmine.stream.operators;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;

public class RedisKeyValueFlatMapFunction<I, O> extends RichFlatMapFunction<I, O> {

    private MapFunction<I, String> keyFunction;
    private MapFunction<Tuple2<I, String>, O> mapFunction;

    private FlinkJedisPoolConfig flinkJedisPoolConfig;
    private JedisPool jedisPool;

    public RedisKeyValueFlatMapFunction(FlinkJedisPoolConfig flinkJedisPoolConfig, MapFunction<I, String> keyFunction, MapFunction<Tuple2<I, String>, O> mapFunction) {
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
        this.jedisPool = RedisKeyValueFlatMapFunction.build(this.flinkJedisPoolConfig);
    }

    public void close() throws IOException {
        if (this.jedisPool != null)
            this.jedisPool.close();
    }

    private String getValueFromRedis(String key) {
        Jedis client = this.jedisPool.getResource();
        String result = client.get(key);
        client.close();
        return result;
    }

    @Override
    public void flatMap(I i, Collector<O> collector) throws Exception {
        String value = this.getValueFromRedis(this.keyFunction.map(i));
        if (value != null)
            collector.collect(this.mapFunction.map(new Tuple2<>(i, value)));
    }
}
