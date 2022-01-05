package zbl.moonlight.server.executor;

/* 可执行接口，用来定义执行器 */
public interface Executable<I, O> extends Runnable {
    void offer(I in);
    O poll();
}
