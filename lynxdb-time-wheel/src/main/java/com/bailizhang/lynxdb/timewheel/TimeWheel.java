package com.bailizhang.lynxdb.timewheel;

public interface TimeWheel {
    long register(TimeoutTask task);
    void unregister(long id);
}
