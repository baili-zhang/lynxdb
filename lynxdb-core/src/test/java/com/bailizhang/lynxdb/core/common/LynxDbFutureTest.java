package com.bailizhang.lynxdb.core.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;

class LynxDbFutureTest {

    @Test
    void test_001() {
        for(int i = 0; i < 20000; i ++) {
            LynxDbFuture<Integer> future = new LynxDbFuture<>();

            final int n = i;
            Thread t1 = new Thread(() -> future.value(n));
            Thread t2 = new Thread(() -> {
                int m = future.get();
                assert m == n;
            });

            t1.start();
            t2.start();
        }
    }

    @Test
    void test_002() {
        for(int i = 0; i < 20000; i ++) {
            LynxDbFuture<Integer> future = new LynxDbFuture<>();

            Thread t1 = new Thread(() -> future.cancel(false));
            Thread t2 = new Thread(() -> Assertions.assertThrows(CancellationException.class, future::get));

            t2.start();
            t1.start();
        }
    }
}