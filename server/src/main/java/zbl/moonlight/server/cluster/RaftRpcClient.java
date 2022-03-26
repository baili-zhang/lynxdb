package zbl.moonlight.server.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.core.executor.Executor;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftRpcClient extends Executor {
    private final static Logger logger = LogManager.getLogger("RaftRpcClient");

    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;
    /** 默认心跳的时间间隔（毫秒数） */
    private final static int DEFAULT_INTERVAL_MILLIS = 3000;

    private final ConcurrentLinkedQueue<RaftNode> nodes = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<RaftNode> failureNodes = new ConcurrentLinkedQueue<>();
    private final ThreadPoolExecutor executor;

    public RaftRpcClient() {
        /* 解析集群各个节点的地址 */
        Configuration config = MdtpServerContext.getInstance()
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

    private void connect(ConcurrentLinkedQueue<RaftNode> nodes, Selector selector) throws IOException {
        while(!nodes.isEmpty()) {
            RaftNode node = nodes.poll();
            SocketAddress address = new InetSocketAddress(node.host(), node.port());
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.connect(address);

            synchronized (selector) {
                socketChannel.register(selector,
                        SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE, node);
            }
        }
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            connect(nodes, selector);
            /* 获取当前的毫秒数 */
            long lastTimeMillis = System.currentTimeMillis();

            while (true) {
                /* 每隔一定时间检查一次 */
                selector.select(DEFAULT_INTERVAL_MILLIS);
                Set<SelectionKey> keys = selector.selectedKeys();
                CountDownLatch latch = new CountDownLatch(keys.size());

                /* 对Set进行迭代，不同步处理的话，可能会出问题 */
                synchronized (selector) {
                    for (SelectionKey key : keys) {
                        try {
                            executor.execute(new RpcClientIoEventHandler(latch, key));
                        } catch (RejectedExecutionException e) {
                            latch.countDown();
                            e.printStackTrace();
                        }
                    }
                }

                latch.await();

                long newTimeMillis = System.currentTimeMillis();
                if(newTimeMillis > lastTimeMillis + DEFAULT_INTERVAL_MILLIS) {
                    RaftState raftState = MdtpServerContext.getInstance().getRaftState();
                    if(raftState.getRaftRole() == RaftRole.Candidate) {
                        System.out.println("发起请求投票" + raftState.getRaftRole());
                    }

                    /* 处理连接失败的节点，进行重连 */
                    if(!failureNodes.isEmpty()) {
                        connect(failureNodes, selector);
                        failureNodes.clear();
                    }

                    /* 重新设置lastTimeMillis，相当于重置定时器 */
                    lastTimeMillis = newTimeMillis;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /* IO线程的线程工厂 */
    private static class RpcClientThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        RpcClientThreadFactory() {
            namePrefix = "Rpc-Client-IO-";
        }

        public Thread newThread(Runnable r) {
            return new Thread(r, namePrefix + threadNumber.getAndIncrement());
        }
    }

    /* 处理IO事件的线程 */
    private class RpcClientIoEventHandler implements Runnable {
        private final CountDownLatch latch;
        private final SelectionKey selectionKey;
        private final EventBus eventBus;

        public RpcClientIoEventHandler(CountDownLatch latch,
                                       SelectionKey selectionKey) {
            this.latch = latch;
            this.selectionKey = selectionKey;
            this.eventBus = MdtpServerContext.getInstance().getEventBus();
        }

        @Override
        public void run() {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            try {
                if(!socketChannel.isConnected()) {
                    if (selectionKey.isConnectable()) {
                        doConnect();
                    }
                } else {
                    if (selectionKey.isReadable()) {
                        doRead();
                    } else if (selectionKey.isWritable()) {
                        doWrite();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        private void doConnect() throws IOException {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            RaftNode node = (RaftNode) selectionKey.attachment();

            try {
                while (!socketChannel.finishConnect()) {
                }
                logger.info("Has connected to raft node {}.", node);
            } catch (ConnectException e) {
                failureNodes.offer(node);
                logger.info("Connect to raft node {} failure.", node);
                return;
            }
            selectionKey.interestOpsAnd(SelectionKey.OP_WRITE);
        }

        private void doRead() throws InterruptedException {
            TimeUnit.MILLISECONDS.sleep(3000);
        }

        private void doWrite() throws InterruptedException {
            TimeUnit.MILLISECONDS.sleep(3000);
        }
    }
}
