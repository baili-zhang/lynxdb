package zbl.moonlight.core.executor;

/* 可执行接口，用来定义执行器 */
public interface Executable extends Runnable {
    void offer(Event event);
}
