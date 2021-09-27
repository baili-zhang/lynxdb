package zbl.moonlight.cache.server;

import zbl.moonlight.cache.server.worker.WorkerPool;
import zbl.moonlight.cache.server.io.ChannelSelector;
import zbl.moonlight.cache.server.config.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MoonlightCacheServer {
    private static final int WORKER_NUMBER = 20;
    private static final Configuration config = new Configuration();

    private static final int PORT = config.getPort();

    public static void main(String[] args) {

        /* create and init a worker pool */
        WorkerPool.init(10, 30, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));

        /* run channel selector thread */
        new Thread(new ChannelSelector(PORT)).start();
    }
}
