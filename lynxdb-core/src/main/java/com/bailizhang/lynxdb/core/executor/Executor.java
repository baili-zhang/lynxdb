package com.bailizhang.lynxdb.core.executor;


import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Executor<E> extends Shutdown implements Executable<E>, Interruptable {
    private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();
    private Thread currentThread;

    public static <E> void start(Executor<E> executor) {
        String name = executor.getClass().getSimpleName();
        start(executor, name);
    }

    public static <E> void start(Executor<E> executor, String name) {
        Thread thread = new Thread(executor, name);
        executor.setThread(thread);
        thread.start();
    }

    public static void startRunnable(Runnable runnable) {
        String name = runnable.getClass().getSimpleName();
        Thread thread = new Thread(runnable, name);
        thread.start();
    }

    @Override
    public final void offer(E e) {
        if(e != null) {
            queue.offer(e);
            synchronized (queue) {
                queue.notify();
            }
        }
    }

    public void offerInterruptibly(E e) {
        offer(e);
        interrupt();
    }

    protected final E blockPoll() {
        if(queue.isEmpty()) {
            synchronized (queue) {
                try {
                    queue.wait();
                } catch (InterruptedException ignored) {}
            }
        }
        return queue.poll();
    }

    protected final E poll() {
        return queue.poll();
    }

    private void setThread(Thread thread) {
        currentThread = thread;
    }

    @Override
    public final void interrupt() {
        currentThread.interrupt();
    }

    @Override
    public final void run() {
        doBeforeExecute();
        while(isNotShutdown()) {
            execute();
        }
        doAfterShutdown();
    }

    protected final void handleShutdown() {
        interrupt();
    }

    protected void doBeforeExecute() {
    }

    protected void doAfterShutdown() {
    }

    protected abstract void execute();
}
