package com.bailizhang.lynxdb.client;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class LynxDbFuture implements Future<byte[]> {

    private final AtomicInteger count = new AtomicInteger(0);
    private final Node header;
    private volatile Node tail;

    private volatile boolean completed = false;
    private volatile byte[] value;

    public LynxDbFuture() {
        Node node = new Node(null);
        header = node;
        tail = node;
    }

    public void value(byte[] val) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return completed;
    }

    @Override
    public byte[] get() {
        while (!completed) {
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
        return value;
    }

    @Override
    public byte[] get(long timeout, TimeUnit unit) {
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

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            TAIL = lookup.findVarHandle(LynxDbFuture.class, "tail", Node.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
