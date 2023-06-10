package com.bailizhang.lynxdb.core.common;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@CheckThreadSafety
public class LynxDbFuture<T> implements Future<T> {

    private final AtomicInteger count = new AtomicInteger(0);
    private final Node header;
    private volatile Node tail;

    private volatile Boolean canceled = null;
    private volatile boolean completed = false;
    private volatile T value;

    public LynxDbFuture() {
        Node node = new Node(null);
        header = node;
        tail = node;
    }

    public void value(T val) {
        if(completed) {
            return;
        }

        value = val;

        if(!completed) {
            completed = true;

            Node node = header;
            while(node.next != null) {
                node = node.next;
                LockSupport.unpark(node.thread);
            }
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancel = CANCELED.compareAndSet(this, null, true);

        Node node = header;
        while(node.next != null) {
            node = node.next;

            if(mayInterruptIfRunning) {
                node.thread.interrupt();
            } else {
                LockSupport.unpark(node.thread);
            }
        }

        return cancel;
    }

    @Override
    public boolean isCancelled() {
        return canceled != null && canceled;
    }

    @Override
    public boolean isDone() {
        return completed;
    }

    @Override
    public T get() {
        while (!completed && !isCancelled()) {
            Thread thread = Thread.currentThread();
            Node node = new Node(thread);
            Node t = tail;

            while(!TAIL.compareAndSet(this, t, node)) {
                Thread.yield();
                t = tail;
            }

            t.next = node;
            node.prev = t;

            count.getAndIncrement();
            LockSupport.park();
        }

        if(isCancelled()) {
            throw new CancellationException();
        }

        return value;
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    public int blockedThreadCount() {
        return count.get();
    }

    private static class Node {
        private Node prev;
        private Node next;
        private final Thread thread;

        Node(Thread thd) {
            thread = thd;
        }
    }

    private static final VarHandle TAIL;
    private static final VarHandle CANCELED;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            TAIL = lookup.findVarHandle(LynxDbFuture.class, "tail", Node.class);
            CANCELED = lookup.findVarHandle(LynxDbFuture.class, "canceled", Boolean.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
