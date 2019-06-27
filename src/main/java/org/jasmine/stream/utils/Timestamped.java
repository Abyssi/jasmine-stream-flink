package org.jasmine.stream.utils;

public class Timestamped<E> {
    private long timestamp;
    private E e;

    public Timestamped(E e, long timestamp) {
        this.e = e;
    }

    public E getE() {
        return e;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
