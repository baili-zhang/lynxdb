package zbl.moonlight.core.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Executor<E> implements Executable<E>, Interruptable {
    private static final Logger logger = LogManager.getLogger("Executor");

    private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();
    private Thread currentThread;

    public static <E> Executor<E> start(Executor<E> executor) {
        String name = executor.getClass().getSimpleName();
        Thread thread = new Thread(executor, name);
        executor.setThread(thread);
        thread.start();

        logger.info("Executor \"{}\" has started.", name);
        return executor;
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

    public final void offerInterruptibly(E e) {
        offer(e);
        interrupt();
    }

    protected final E blockPoll() {
        if(queue.isEmpty()) {
            synchronized (queue) {
                try {
                    queue.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
    public void interrupt() {
        currentThread.interrupt();
    }
}
