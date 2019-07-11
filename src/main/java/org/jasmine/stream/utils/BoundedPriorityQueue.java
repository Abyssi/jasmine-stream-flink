package org.jasmine.stream.utils;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue<E> extends PriorityQueue<E> implements Serializable, Cloneable {

    private int maxItems;

    public BoundedPriorityQueue(int maxItems, Comparator<? super E> comparator) {
        super(comparator);
        this.maxItems = maxItems;
    }

    public BoundedPriorityQueue(int maxItems, PriorityQueue<E> queue) {
        super(queue);
        this.maxItems = maxItems;
    }

    @Override
    public boolean add(E e) {
        if (e == null) return false;
        try {
            boolean success = this.offer(e);
            if (!success) return false;
            else if (this.size() > maxItems) //this.poll();
            return true;
        } catch (Exception error) {
            System.out.println("abc: " +e);
            System.out.println("abcdef: " +this);
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
        //System.out.println(this);
        return (E[]) Array.newInstance(eClass, this.size());
        /*
        E[] array = this.toArray((E[]) Array.newInstance(eClass, this.size()));
        Arrays.sort(array, this.comparator().reversed());
        return array;
        */
    }

    @Override
    public BoundedPriorityQueue<E> clone() {
        return new BoundedPriorityQueue<>(this.maxItems, this);
    }
}
