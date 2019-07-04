package org.jasmine.stream.utils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue<E> extends PriorityQueue<E> implements Serializable {

    private int maxItems;

    public BoundedPriorityQueue(int maxItems, Comparator<? super E> comparator) {
        super(comparator);
        this.maxItems = maxItems;
    }

    @Override
    public boolean offer(E e) {
        if (e == null) return false;
        try {
            boolean success = super.offer(e);
            if (!success) return false;
            else if (this.size() > maxItems) this.poll();
            return true;
        } catch (Exception error) {
            System.out.println("Error: " + error);
        }
        return false;
    }

    public void merge(BoundedPriorityQueue<E> other) {
        this.addAll(other);
    }

    @SuppressWarnings("unchecked")
    public E[] toSortedArray() {
        E[] array = (E[]) this.toArray();
        Arrays.sort(array, this.comparator().reversed());
        return array;
    }

    @SuppressWarnings("unchecked")
    public E[] toSortedArray(Class<E> eClass) {
        E[] array = this.toArray((E[]) Array.newInstance(eClass, this.size()));
        Arrays.sort(array, this.comparator().reversed());
        return array;
    }
}
