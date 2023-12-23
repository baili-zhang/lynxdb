/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    /**
     * 线程等待，但是线程还未加入等待队列
     */
    private static final int WAITING = 1;
    /**
     * 线程等待，线程已经加入等待队列
     */
    private static final int WAITED = 2;
    /**
     * 完成，但是还未设置值
     */
    private static final int COMPLETED = 3;
    /**
     * 已取消
     */
    private static final int CANCELED = 4;
    /**
     * 完成，并且已经设置值
     */
    private static final int FINAL = 5;

    private volatile int state = 0;
    private Thread waiterThread = null;
    private T value;

    public LynxDbFuture() {
    }

    public void value(T val) {
        while (true) {
            if(state == INIT && STATE.compareAndSet(this, INIT, COMPLETED)) {
                value = val;
                STATE.setRelease(this, FINAL);

                // 用于确认
                assert waiterThread == null;

                return;
            }

            // 说明 waiterThread 还未设置完成
            if(state == WAITING) {
                Thread.yield();
                continue;
            }

            if(state == WAITED && STATE.compareAndSet(this, WAITED, COMPLETED)) {
                value = val;
                STATE.setRelease(this, FINAL);

                // 用于确认
                assert waiterThread != null;

                LockSupport.unpark(waiterThread);
                return;
            }

            // 如果变成其他状态，报错
            throw new RuntimeException("Has already set value");
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        while (true) {
            if(state == INIT && STATE.compareAndSet(this, INIT, CANCELED)) {
                assert waiterThread == null;
                return true;
            }

            // 说明 waiterThread 还未设置完成
            if(state == WAITING) {
                Thread.yield();
                continue;
            }

            if(state == WAITED && STATE.compareAndSet(this, WAITED, CANCELED)) {
                assert waiterThread != null;
                LockSupport.unpark(waiterThread);
                return true;
            }

            // 如果变成其他状态，报错
            throw new RuntimeException();
        }
    }

    @Override
    public boolean isCancelled() {
        return state == CANCELED;
    }

    @Override
    public boolean isDone() {
        return state == FINAL;
    }

    @Override
    public T get() {
        while (true) {
            if(waiterThread != null && waiterThread != Thread.currentThread()) {
                throw new RuntimeException();
            }

            if(state == INIT && STATE.compareAndSet(this, INIT, WAITING)) {
                waiterThread = Thread.currentThread();
                STATE.setRelease(this, WAITED);
                LockSupport.park();
            }

            if(state == COMPLETED) {
                Thread.yield();
                continue;
            }

            if(state == FINAL) {
                return value;
            }

            if(state == CANCELED) {
                throw new CancellationException();
            }
        }
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
