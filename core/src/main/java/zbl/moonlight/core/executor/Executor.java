package zbl.moonlight.core.executor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class Executor implements Executable {
    private static final Logger logger = LogManager.getLogger("Executor");

    /* 输入队列 */
    private final ConcurrentLinkedQueue<Event> queue = new ConcurrentLinkedQueue<>();

    public static Executable start(Executable executable) {
        String name = executable.getClass().getSimpleName();
        new Thread(executable, name).start();
        logger.info("Executor \"{}\" has started.", name);
        return executable;
    }

    @Override
    /* 向输入队列中添加元素 */
    public final void offer(Event event) {
        if(event != null) {
            queue.offer(event);
            synchronized (queue) {
                queue.notify();
            }
        }
    }

    /* 向输入队列中移除元素，如果没有输入队列元素，则进入等待 */
    protected final Event pollSleep() {
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
    protected final Event poll() {
        return queue.poll();
    }

    /* 判断输入队列是否为空 */
    protected final boolean isEmpty() {
        return queue.isEmpty();
    }
}
