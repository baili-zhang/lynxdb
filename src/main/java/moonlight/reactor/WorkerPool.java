package moonlight.reactor;

import java.util.concurrent.*;

public class WorkerPool {
    private int corePoolSize;
    private int maximumPoolSize;
    private long keepAliveTime;
    private TimeUnit unit;
    private BlockingQueue<Runnable> workQueue;
    private ThreadFactory threadFactory;
    private RejectedExecutionHandler handler;


    private ThreadPoolExecutor threadPoolExecutor;
    private static WorkerPool workerPool;

    private WorkerPool(
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue
    ) {
        threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public static void init (
            int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue
    ) {
        if(workerPool == null) {
            workerPool = new WorkerPool(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }
    }

    public static WorkerPool getInstance() {
        return workerPool;
    }

    public void execute (Runnable task) {
        threadPoolExecutor.execute(task);
    }
}
