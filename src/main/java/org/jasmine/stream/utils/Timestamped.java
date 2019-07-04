package org.jasmine.stream.utils;

import java.io.Serializable;

public class Timestamped<E> implements Serializable, JSONStringable {
    private long timestamp;
    private E element;

    public Timestamped(E element, long timestamp) {
        this.element = element;
        this.timestamp = timestamp;
    }

    public E getElement() {
        return element;
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
