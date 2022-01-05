package zbl.moonlight.server.executor;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public abstract class Executor<I, O> implements Executable<I, O> {
    /* 输入队列 */
    private final ConcurrentLinkedQueue<I> inQueue = new ConcurrentLinkedQueue<>();
    /* 输出队列 */
    private final ConcurrentLinkedQueue<O> outQueue = new ConcurrentLinkedQueue<>();
    /* 执行完需要被通知的线程 */
    private final Thread notifiedThread;

    protected Executor(Thread notifiedThread) {
        this.notifiedThread = notifiedThread;
    }

    @Override
    /* 向输入队列中添加元素 */
    public final void offer(I in) {
        if(in != null) {
            inQueue.offer(in);
        }
    }

    @Override
    /* 从输出队列队首移除元素 */
    public final O poll() {
        return outQueue.poll();
    }

    /* 向输出队列中添加元素 */
    protected final void offerOut(O out) {
        outQueue.offer(out);
        if(Thread.State.TIMED_WAITING.equals(notifiedThread.getState())) {
            notifiedThread.interrupt();
        }
    }

    /* 向输入队列中移除元素，如果没有输入队列元素，则睡眠指定时间 */
    protected final I pollIn() {
        if(inQueue.isEmpty()) {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return inQueue.poll();
    }
}
