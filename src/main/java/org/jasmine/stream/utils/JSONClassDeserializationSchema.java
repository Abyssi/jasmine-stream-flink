package org.jasmine.stream.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JSONClassDeserializationSchema<T> implements DeserializationSchema<T> {
    private static final long serialVersionUID = 1509391548173891955L;
    private Class<T> itemClass;
    private ObjectMapper mapper;

    public JSONClassDeserializationSchema(Class<T> itemClass) {
        this.itemClass = itemClass;
    }

    public T deserialize(byte[] bytes) {
        if (this.mapper == null)
            this.mapper = new ObjectMapper();
        try {
            return this.mapper.readValue(bytes, this.itemClass);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(itemClass);
    }
}
