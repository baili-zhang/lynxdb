package zbl.moonlight.core.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* 可执行接口，用来定义执行器 */
public interface Executable<E> extends Runnable {
    Logger logger = LogManager.getLogger("Executable");

    static <E> Executable<E> start(Executable<E> executable) {
        String name = executable.getClass().getSimpleName();
        new Thread(executable, name).start();

        logger.info("Executor \"{}\" has started.", name);
        return executable;
    }

    void offer(E e);
}
