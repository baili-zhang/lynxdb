package com.bailizhang.lynxdb.core.common;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@CheckThreadSafety("finished")
public class LynxDbFuture<T> implements Future<T> {
    private static final int INIT = 0;
    private static final int WAITING = 1;
    private static final int WAITED = 2;
    private static final int COMPLETED = 3;
    private static final int CANCELED = 4;
    private static final int FINAL = 5;

    private volatile int state = 0;
    private Thread waiterThread = null;
    private T value;

    public LynxDbFuture() {
    }

    public void value(T val) {
        // 不为初始状态
        if(state != INIT || !STATE.compareAndSet(this, INIT, COMPLETED)) {
            while (true) {
                if(state == WAITING) {
                    Thread.yield();
                    continue;
                }

                if(state == WAITED) {
                    if(!STATE.compareAndSet(this, WAITED, COMPLETED)) {
                        continue;
                    }

                    value = val;
                    STATE.setRelease(this, FINAL);
                    LockSupport.unpark(waiterThread);
                    return;
                }

                throw new RuntimeException();
            }
        }

        value = val;
        STATE.setRelease(this, FINAL);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // 如果当前状态不为 INIT
        if(state != INIT || !STATE.compareAndSet(this, INIT, COMPLETED)) {
            while (true) {
                if(state == WAITING) {
                    Thread.yield();
                    continue;
                }

                if(state == WAITED) {
                    if(!STATE.compareAndSet(this, WAITED, COMPLETED)) {
                        continue;
                    }

                    STATE.setRelease(this, CANCELED);
                    LockSupport.unpark(waiterThread);
                    return true;
                }

                throw new RuntimeException();
            }
        }

        STATE.setRelease(this, CANCELED);
        return true;
    }

    @Override
    public boolean isCancelled() {
        return state == CANCELED;
    }

    @Override
    public boolean isDone() {
        return state == COMPLETED;
    }

    @Override
    public T get() {
        if(waiterThread != null && waiterThread != Thread.currentThread()) {
            throw new RuntimeException();
        }

        if(STATE.compareAndSet(this, INIT, WAITING)) {
            waiterThread = Thread.currentThread();
            STATE.setRelease(this, WAITED);
            LockSupport.park();
        }

        while (state == COMPLETED) {
            Thread.yield();
        }

        return switch (state) {
            case FINAL -> value;
            case CANCELED -> throw new CancellationException();
            default -> throw new RuntimeException();
        };
    }

    @Override
    public T get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }

    private static final VarHandle STATE;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            STATE = lookup.findVarHandle(LynxDbFuture.class, "state", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
