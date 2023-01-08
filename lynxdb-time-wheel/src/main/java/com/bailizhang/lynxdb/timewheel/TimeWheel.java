package com.bailizhang.lynxdb.timewheel;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeWheel {

    /** 时间轮的刻度值 */
    private final int scale;
    private final TimeWheel next;
    private final List<TimeoutTask>[] circle;
    /** 当前的刻度值 */
    private final AtomicInteger pointer = new AtomicInteger();

    @SuppressWarnings("unchecked")
    public TimeWheel(int scale, TimeWheel next) {
        this.scale = scale;
        this.next = next;

        circle = new List[scale];
    }

    public TimeWheel next() {
        return next;
    }

    public boolean hasNoNext() {
        return next == null;
    }

    public void init(long timestamp) {

    }
}
