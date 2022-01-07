package zbl.moonlight.server.executor;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Executor<E> implements Executable<E> {
    /* 输入队列 */
    private final ConcurrentLinkedQueue<E> queue = new ConcurrentLinkedQueue<>();
    /* 下游的执行器 */
    private final Executable<E> downStreamExecutor;

    protected Executor(Executable<E> downStreamExecutor) {
        this.downStreamExecutor = downStreamExecutor;
    }

    @Override
    /* 向输入队列中添加元素 */
    public final void offer(E event) {
        if(event != null) {
            queue.offer(event);
        }
    }

    /* 向输入队列中移除元素，如果没有输入队列元素，则进入等待 */
    protected final E pollSleep() {
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

    /* 从输入队列中移除元素 */
    protected final E poll() {
        return queue.poll();
    }

    /* 把事件发往下游，通知下游线程 */
    protected final void send(E event) {
        downStreamExecutor.offer(event);
        synchronized (queue) {
            queue.notify();
        }
    }

    /* 判断输入队列是否为空 */
    protected final boolean isEmpty() {
        return queue.isEmpty();
    }
}
