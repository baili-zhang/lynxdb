package zbl.moonlight.core.executor;

/* TODO: 把一些在类中独立实现的 shutdown 逻辑替换为继承 Shutdown 类 */
public abstract class Shutdown {
    private volatile boolean shutdown = false;
    private volatile boolean shutdownFinish = false;

    public void shutdown() {
        shutdown = true;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isShutdownFinish() {
        return shutdownFinish;
    }
}
