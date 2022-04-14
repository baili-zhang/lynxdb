package zbl.moonlight.server.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpResponse;
import zbl.moonlight.server.mdtp.MdtpResponseSchema;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.core.executor.Executor;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftRpcClient extends Executor {
    private final static Logger logger = LogManager.getLogger("RaftRpcClient");

    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;

    private final RaftState raftState;
    private final Configuration config;

    private final ConcurrentLinkedQueue<RaftNode> nodes = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<RaftNode> failureNodes = new ConcurrentLinkedQueue<>();
    /** 已经连接上的节点 */
    private final ConcurrentHashMap<SelectionKey, RaftNode> connected = new ConcurrentHashMap<>();
    /** 请求投票中 */
    private final Set<SelectionKey> voting = new HashSet<>();
    /** 任务事件队列 */
    private final ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<NioWriter>> jobsQueueMap
            = new ConcurrentHashMap<>();
    /** 保存每个channel对应的reader */
    private final ConcurrentHashMap<SelectionKey, NioReader> readers = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;

    public RaftRpcClient() {
        MdtpServerContext context = MdtpServerContext.getInstance();

        /* 服务器相关配置的信息 */
        config = context.getConfiguration();

        /* Raft集群相关的信息 */
        raftState = context.getRaftState();

        /* 解析集群各个节点的地址 */
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
                        SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                        node);
            }
        }
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            connect(nodes, selector);

            while (true) {
                /* 每隔一定时间检查一次 */
                selector.select(RaftState.HEARTBEAT_INTERVAL_MILLIS);
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = keys.iterator();
                CountDownLatch latch = new CountDownLatch(keys.size());

                /* 对Set进行迭代，不同步处理的话，可能会出问题 */
                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        try {
                            executor.execute(new RpcClientIoEventHandler(latch, key));
                        } catch (RejectedExecutionException e) {
                            latch.countDown();
                            e.printStackTrace();
                        }
                        iterator.remove();
                    }
                }

                latch.await();

                /* 从队列里拿出一个事件 */
                Event event = poll();


                if(System.currentTimeMillis() > raftState.getHeartbeatTimeMillis()
                        + RaftState.HEARTBEAT_INTERVAL_MILLIS) {
                    /* 处理连接失败的节点，进行重连 */
                    if(!failureNodes.isEmpty()) {
                        connect(failureNodes, selector);
                        failureNodes.clear();
                    }

                    if(event == null && raftState.getRaftRole() == RaftRole.Leader) {
                        for(SelectionKey key : jobsQueueMap.keySet()) {
                            ConcurrentLinkedQueue<NioWriter> queue = jobsQueueMap.get(key);
                            queue.offer(RaftRpc.newAppendEntries(key));
                            key.interestOpsOr(SelectionKey.OP_WRITE);
                        }
                    }

                    /* 重新设置lastTimeMillis，相当于重置定时器 */
                    raftState.setHeartbeatTimeMillis(System.currentTimeMillis());
                }

                if(System.currentTimeMillis() >
                        raftState.getTimeoutTimeMillis() + RaftState.TIMEOUT_MILLIS
                        && raftState.getRaftRole() != RaftRole.Leader) {
                    /* 发起请求投票 */
                    requestVote();
                    /* 重新设置lastTimeMillis，相当于重置定时器 */
                    raftState.setTimeoutTimeMillis(System.currentTimeMillis());
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /** 请求投票的响应 */
    private void requestVote() {
        raftState.setRaftRole(RaftRole.Candidate);
        raftState.setCurrentTerm(raftState.getCurrentTerm() + 1);
        raftState.setVotedFor(new RaftNode(config.getHost(), config.getPort()));

        synchronized (voting) {
            voting.clear();
        }

        for (SelectionKey key : connected.keySet()) {
            jobsQueueMap.get(key).offer(RaftRpc.newRequestVote(key));
            synchronized (voting) {
                voting.add(key);
            }
            key.interestOpsOr(SelectionKey.OP_WRITE);
        }

        logger.info("Initiate a request to vote, Current Term is: {}", raftState.getCurrentTerm());
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
            connected.put(selectionKey, node);
            readers.put(selectionKey, new NioReader(MdtpResponseSchema.class, selectionKey));
            jobsQueueMap.put(selectionKey, new ConcurrentLinkedQueue<>());
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }

        private void doRead() throws IOException {
            NioReader reader = readers.get(selectionKey);

            try {
                reader.read();
            } catch (SocketException e) {
                handleDisconnect();
                e.printStackTrace();
            }

            if(reader.isReadCompleted()) {
                handleRequestVote(reader);
                readers.put(selectionKey, new NioReader(MdtpResponseSchema.class, selectionKey));
                return;
            }
        }

        private void doWrite() throws IOException {
            ConcurrentLinkedQueue<NioWriter> queue = jobsQueueMap.get(selectionKey);
            NioWriter writer = queue.peek();

            if(writer == null) return;
            writer.write();

            if(writer.isWriteCompleted()) {
                queue.poll();
                if(queue.isEmpty()) {
                    /* 只监听读 */
                    selectionKey.interestOpsAnd(SelectionKey.OP_READ);
                } else {
                    /* 监听读写 */
                    selectionKey.interestOpsOr(SelectionKey.OP_READ);
                }
            }
        }

        /** 处理节点断开连接的情况 */
        private void handleDisconnect() {
            failureNodes.add((RaftNode) selectionKey.attachment());
            selectionKey.cancel();
            connected.remove(selectionKey);
            jobsQueueMap.remove(selectionKey);
            logger.info("Raft Node {} is disconnect.", selectionKey.attachment());
        }

        /** 处理选举请求 */
        private void handleRequestVote(NioReader reader) {
            MdtpResponse response = new MdtpResponse(reader);
            logger.info("Raft client has received response: {}", response);

            /* 如果请求投票响应成功 */
            if(response.status() == ResponseStatus.GET_VOTE) {
                synchronized (voting) {
                    voting.remove(selectionKey);
                }
                /* 接受到一半以上的选票，成为Leader */
                if(voting.size() < (nodes.size() >> 1) || voting.size() == 0) {
                    raftState.setRaftRole(RaftRole.Leader);
                    logger.info("Get most votes, Change role to [RaftRole.Leader], Current term is {}.", raftState.getCurrentTerm());
                }
            }
        }
    }
}
