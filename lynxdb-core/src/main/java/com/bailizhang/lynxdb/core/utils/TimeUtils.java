package com.bailizhang.lynxdb.core.utils;

import java.util.concurrent.TimeUnit;

public interface TimeUtils {
    static void sleep(TimeUnit timeUnit, long timeout) {
        // 不允许被中断的 sleep
        try {
            timeUnit.sleep(timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
