package com.bailizhang.lynxdb.socket.timewheel;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

public class SocketTimeWheel {
    private static final SocketTimeWheel socketTimeWheel = new SocketTimeWheel();

    private final LynxDbTimeWheel timeWheel;

    public static SocketTimeWheel timeWheel() {
        return socketTimeWheel;
    }

    private SocketTimeWheel() {
        timeWheel = new LynxDbTimeWheel();
    }

    public TimeoutTask register(long time, Runnable runnable) {
        return register(time, null, runnable);
    }

    public TimeoutTask register(long time, Object identifier, Runnable runnable) {
        TimeoutTask task = new TimeoutTask(time, identifier, runnable);
        timeWheel.register(task);
        return task;
    }

    public void start() {
        Executor.startRunnable(timeWheel);
    }
}
