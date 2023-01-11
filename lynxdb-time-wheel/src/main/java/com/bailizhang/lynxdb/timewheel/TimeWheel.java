package com.bailizhang.lynxdb.timewheel;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TimeWheel {

    /** 时间轮的刻度值 */
    private final int scale;
    private final TimeWheel next;
    private final List<TimeoutTask>[] circle;
    /** 当前的刻度值 */
    private final AtomicLong pointer = new AtomicLong();

    @SuppressWarnings("unchecked")
    public TimeWheel(int scale, TimeWheel next) {
        this.scale = scale;
        this.next = next;

        circle = new List[scale];
    }

    public boolean hasNoNext() {
        return next == null;
    }

    public void init(int val) {
        if(val < scale) {
            pointer.set(val);
            return;
        }

        if(next != null) {
            next.init(val % scale);
        }
    }

    public void tick() {

    }
}
