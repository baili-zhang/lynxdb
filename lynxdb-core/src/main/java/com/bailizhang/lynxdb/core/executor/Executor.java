package com.bailizhang.lynxdb.core.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Executor<E> extends Shutdown implements Executable<E>, Interruptable {
    private static final Logger logger = LogManager.getLogger("Executor");

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

        logger.info("Executor \"{}\" has started.", name);
    }

    public static void start(Runnable executor) {
        String name = executor.getClass().getSimpleName();
        Thread thread = new Thread(executor, name);
        thread.start();
        logger.info("Executor \"{}\" has started.", name);
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
        doAfterExecute();
    }

    protected final void doAfterShutdown() {
        interrupt();
    }

    protected void doBeforeExecute() {
    }

    protected void doAfterExecute() {
    }

    protected abstract void execute();
}
