package zbl.moonlight.server.executor;

/* 可执行接口，用来定义执行器 */
public interface Executable<E> extends Runnable {
    void offer(E event);
}
