package com.bailizhang.lynxdb.timewheel.task;

import java.util.Arrays;
import java.util.Objects;

public class TimeoutTask implements Comparable<TimeoutTask> {
    private final long time;

    private final byte[] identifier;

    private final Runnable runnableTask;

    public TimeoutTask(long time, Runnable runnableTask) {
        this(time, null, runnableTask);
    }

    public TimeoutTask(long time, byte[] identifier, Runnable runnableTask) {
        this.time = time;
        this.runnableTask = runnableTask;
        this.identifier = identifier;
    }

    public long time() {
        return time;
    }

    public byte[] identifier() {
        return identifier;
    }

    public void doTask() {
        runnableTask.run();
    }

    @Override
    public String toString() {
        String template = "{ time: %s, task: %s }";
        return String.format(template, time, runnableTask);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeoutTask task = (TimeoutTask) o;
        return time == task.time && Arrays.equals(identifier, task.identifier);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(time);
        result = 31 * result + Arrays.hashCode(identifier);
        return result;
    }

    @Override
    public int compareTo(TimeoutTask o) {
        return Long.compare(time, o.time());
    }
}
