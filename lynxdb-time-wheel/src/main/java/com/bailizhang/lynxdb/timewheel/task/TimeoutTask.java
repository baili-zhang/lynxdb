package com.bailizhang.lynxdb.timewheel.task;

public class TimeoutTask {
    private final long time;

    private final Runnable runnableTask;

    public TimeoutTask(long time, Runnable runnableTask) {
        this.time = time;
        this.runnableTask = runnableTask;
    }

    public long time() {
        return time;
    }

    public void doTask() {
        runnableTask.run();
    }

    @Override
    public String toString() {
        String template = "{ time: %s, task: %s }";
        return String.format(template, time, runnableTask);
    }
}
