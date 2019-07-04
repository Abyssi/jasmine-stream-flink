package org.jasmine.stream.utils;

import java.io.Serializable;

public class Timestamped<E> implements Serializable, JSONStringable {
    private long timestamp;
    private E e;

    public Timestamped(E e, long timestamp) {
        this.e = e;
        this.timestamp = timestamp;
    }

    public E getElement() {
        return e;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }
}
