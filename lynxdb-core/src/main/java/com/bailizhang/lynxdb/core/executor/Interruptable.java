package com.bailizhang.lynxdb.core.executor;

public interface Interruptable {
    /**
     * 中断执行器的线程
     */
    void interrupt();
}
