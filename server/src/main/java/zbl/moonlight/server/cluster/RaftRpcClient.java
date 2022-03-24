package zbl.moonlight.server.cluster;

import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.executor.Executor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftRpcClient extends Executor {
    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final ThreadPoolExecutor executor;

    public RaftRpcClient() {
        /* 解析集群各个节点的地址 */
        Configuration config = ServerContext.getInstance()
                .getConfiguration();
        String nativeHost = config.getHost();
        int nativePort = config.getPort();
        ClusterConfiguration clusterConfig = config.getClusterConfiguration();
        for(LinkedHashMap<String, Object> node : clusterConfig.nodes()) {
            /* TODO:禁止魔法值（“host”,"port"） RaftNode列表应该放到Configuration中解析 */
            String host = (String) node.get("host");
            int port = (int) node.get("port");
            if(nativeHost.equals(host) && nativePort == port) continue;
            nodes.add(new RaftNode(host, port));
        }

        /* 线程池执行器 */
        executor = new ThreadPoolExecutor(nodes.size(),
                nodes.size(),
                DEFAULT_KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(DEFAULT_CAPACITY),
                new RpcClientThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            for(RaftNode node : nodes) {
                SocketAddress address = new InetSocketAddress(node.host(), node.port());
                SocketChannel socketChannel = SocketChannel.open(address);
                socketChannel.register(selector, SelectionKey.OP_CONNECT);
            }

            /* Leader选举的投票请求（包括心跳） */
            while (true) {
                selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                CountDownLatch latch = new CountDownLatch(keys.size());

                for(SelectionKey key : keys) {
                    try {
                        executor.execute(new RpcClientIoEventHandler(latch, selector, key));
                    } catch (RejectedExecutionException e) {
                        latch.countDown();
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* IO线程的线程工厂 */
    private static class RpcClientThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        RpcClientThreadFactory() {
            namePrefix = "Rpc-Client" + "-Thread-";
        }

        public Thread newThread(Runnable r) {
            return new Thread(r, namePrefix + threadNumber.getAndIncrement());
        }
    }

    /* 处理IO事件的线程 */
    private static class RpcClientIoEventHandler implements Runnable {
        private final CountDownLatch latch;
        private final Selector selector;
        private final SelectionKey selectionKey;
        private final EventBus eventBus;

        public RpcClientIoEventHandler(CountDownLatch latch,
                                       Selector selector,
                                       SelectionKey selectionKey) {
            this.latch = latch;
            this.selector = selector;
            this.selectionKey = selectionKey;
            this.eventBus = ServerContext.getInstance().getEventBus();
        }

        @Override
        public void run() {
            try {
                if (selectionKey.isConnectable()) {
                    doConnect();
                } else if (selectionKey.isReadable()) {
                    doRead();
                } else if (selectionKey.isWritable()) {
                    doWrite();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        private void doConnect() {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        }

        private void doRead() {
        }

        private void doWrite() {
        }
    }
}
