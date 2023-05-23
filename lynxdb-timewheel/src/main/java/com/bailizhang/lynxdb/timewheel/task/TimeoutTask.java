package com.bailizhang.lynxdb.timewheel.task;

import java.util.Objects;

public class TimeoutTask implements Comparable<TimeoutTask> {
    private final long time;

    private final Object identifier;

    private final Runnable runnableTask;

    public TimeoutTask(long time, Runnable runnableTask) {
        this(time, null, runnableTask);
    }

    public TimeoutTask(long time, Object identifier, Runnable runnableTask) {
        this.time = time;
        this.runnableTask = runnableTask;
        this.identifier = identifier;
    }

    public long time() {
        return time;
    }

    public Object identifier() {
        return identifier;
    }

    public Runnable runnableTask() {
        return runnableTask;
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
    public int compareTo(TimeoutTask o) {
        return Long.compare(time, o.time());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeoutTask task = (TimeoutTask) o;
        return time == task.time && Objects.equals(identifier, task.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(time, identifier);
    }
}
