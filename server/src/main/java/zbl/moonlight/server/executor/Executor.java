package zbl.moonlight.server.executor;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public abstract class Executor<E> implements Executable<E> {
    /* 输入队列 */
    private final ConcurrentLinkedQueue<E> inQueue = new ConcurrentLinkedQueue<>();
    /* 需要被通知的下游线程 */
    private final Thread downStreamThread;
    /* 下游的执行器 */
    private final Executable<E> downStreamExecutor;

    protected Executor(Executable<E> downStreamExecutor, Thread downStreamThread) {
        this.downStreamExecutor = downStreamExecutor;
        this.downStreamThread = downStreamThread;
    }

    @Override
    /* 向输入队列中添加元素 */
    public final void offer(E event) {
        if(event != null) {
            inQueue.offer(event);
        }
    }

    /* 向输入队列中移除元素，如果没有输入队列元素，则睡眠指定时间 */
    protected final E pollInSleep() {
        if(inQueue.isEmpty()) {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return inQueue.poll();
    }

    /* 向输入队列中移除元素, 不睡眠 */
    protected final E pollIn() {
        return inQueue.poll();
    }

    /* 把事件发往下游，通知下游线程 */
    protected final void send(E event) {
        downStreamExecutor.offer(event);
        if(Thread.State.TIMED_WAITING.equals(downStreamThread.getState())) {
            downStreamThread.interrupt();
        }
    }
}
