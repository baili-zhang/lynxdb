package com.bailizhang.lynxdb.core.recorder;

import java.util.concurrent.Callable;

public interface Recorder {
    static long runningTime(Callable<?> task) throws Exception {
        long beginTime = System.currentTimeMillis();
        task.call();
        long endTime = System.currentTimeMillis();
        return endTime - beginTime;
    }
}
