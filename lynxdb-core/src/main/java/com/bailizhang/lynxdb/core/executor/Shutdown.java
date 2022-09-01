package com.bailizhang.lynxdb.core.executor;

/* TODO: 把一些在类中独立实现的 shutdown 逻辑替换为继承 Shutdown 类 */
public abstract class Shutdown {
    private volatile boolean shutdown = false;

    public void shutdown() {
        doAfterShutdown();
        shutdown = true;
    }

    public boolean isNotShutdown() {
        return !shutdown;
    }
    protected abstract void doAfterShutdown();
}
