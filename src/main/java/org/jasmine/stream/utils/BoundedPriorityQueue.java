package org.jasmine.stream.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class BoundedPriorityQueue<E> extends PriorityQueue<E> {

    private int maxItems;

    public BoundedPriorityQueue(int maxItems, Comparator<? super E> comparator) {
        super(comparator);
        this.maxItems = maxItems;
    }

    @Override
    public boolean offer(E e) {
        boolean success = super.offer(e);
        if (!success) return false;
        else if (this.size() > maxItems) this.poll();
        //else if (this.size() > maxItems) this.remove(this.toArray()[this.size()-1]);
        return true;
    }

    public void merge(BoundedPriorityQueue<E> other) {
        this.addAll(other);
    }

    @SuppressWarnings("unchecked")
    @Override
    public E[] toArray() {
        List<E> list = new ArrayList<>(this);
        list.sort(this.comparator());
        return (E[]) list.toArray();
    }
}
