package org.jasmine.stream.utils;

import java.io.Serializable;
import java.util.concurrent.Callable;

public abstract class SerializableCallback<T> implements Serializable, Callable<T> {
    public T callNoExc() {
        try {
            return this.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
