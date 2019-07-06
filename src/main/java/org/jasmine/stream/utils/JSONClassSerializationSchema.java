package org.jasmine.stream.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JSONClassSerializationSchema<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1509391548173891955L;
    private ObjectMapper mapper;

    @Override
    public byte[] serialize(T t) {
        if (this.mapper == null)
            this.mapper = new ObjectMapper();
        try {
            return this.mapper.writeValueAsBytes(t);
        } catch (Exception e) {
            return null;
        }
    }
}
