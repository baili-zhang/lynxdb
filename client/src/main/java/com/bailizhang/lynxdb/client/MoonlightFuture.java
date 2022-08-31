package com.bailizhang.lynxdb.client;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class MoonlightFuture implements Future<byte[]> {
    private final Thread current;

    private volatile boolean completed = false;
    private volatile byte[] value;

    public MoonlightFuture() {
        current = Thread.currentThread();
    }

    public void value(byte[] val) {
        value = val;

        if(!completed) {
            completed = true;
            LockSupport.unpark(current);
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
            LockSupport.park();
        }
        return value;
    }

    @Override
    public byte[] get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
