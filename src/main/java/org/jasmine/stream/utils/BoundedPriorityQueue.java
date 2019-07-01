package org.jasmine.stream.utils;

import java.util.Comparator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue<E> extends PriorityQueue<E> {

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
            //else if (this.size() > maxItems) this.remove(this.toArray()[this.size()-1]);
            return true;
        } catch (Exception error) {
            System.out.println(e);
        }
        return false;
    }

    public void merge(BoundedPriorityQueue<E> other) {
        this.addAll(other);
    }

    @SuppressWarnings("unchecked")
    public E[] toSortedArray() {
        return (E[]) this.toArray();
    }
}
