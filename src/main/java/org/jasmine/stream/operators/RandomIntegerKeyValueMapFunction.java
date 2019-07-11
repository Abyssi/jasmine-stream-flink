package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Random;

public class RandomIntegerKeyValueMapFunction<I> implements MapFunction<I, Tuple2<Integer, I>> {
    private Random random = new Random();
    private int max;

    public RandomIntegerKeyValueMapFunction(int max) {
        this.max = max;
    }

    @Override
    public Tuple2<Integer, I> map(I i) throws Exception {
        return new Tuple2<>(random.nextInt(max + 1), i);
    }
}
