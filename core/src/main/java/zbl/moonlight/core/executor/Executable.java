package zbl.moonlight.core.executor;

/* 可执行接口，用来定义执行器 */
public interface Executable<E> extends Runnable {
    /**
     * 向任务队列中添加元素
     * @param e
     */
    void offer(E e);
}
