package org.jasmine.stream.utils;

import java.io.Serializable;

public class Identified<ID, E> implements Serializable, JSONStringable {
    private ID id;
    private E element;

    public Identified(ID id, E element) {
        this.id = id;
        this.element = element;

    }

    public E getElement() {
        return element;
    }

    public ID getId() {
        return id;
    }

    public void setId(ID id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return this.toJSONString();
    }

    public static class ByInteger<E> extends Identified<Integer, E> {
        public ByInteger(Integer integer, E element) {
            super(integer, element);
        }
    }
}
