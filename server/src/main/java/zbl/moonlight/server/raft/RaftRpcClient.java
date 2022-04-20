package zbl.moonlight.server.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequest;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.core.executor.Executor;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
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
    private final ConcurrentHashMap<SelectionKey, RaftRpcClientContext> contexts = new ConcurrentHashMap<>();
    private final ThreadPoolExecutor executor;

    public RaftRpcClient() {
        MdtpServerContext context = MdtpServerContext.getInstance();

        /* 服务器相关配置的信息 */
        config = context.getConfiguration();

        /* Raft集群相关的信息 */
        raftState = context.getRaftState();

        /* 不包含当前节点的其他节点 */
        nodes.addAll(raftState.getRaftNodes());

        logger.debug("Raft node need to connect: {}", nodes);

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
                        for(SelectionKey key : contexts.keySet()) {
                            ConcurrentLinkedQueue<NioWriter> queue = contexts.get(key).getWriters();
                            queue.offer(RaftRpc.newHeartBeat(key));
                            key.interestOpsOr(SelectionKey.OP_WRITE);
                        }
                        logger.info("Send heartbeat to followers.");
                    }

                    while(event != null) {
                        MdtpRequest mdtpRequest = new MdtpRequest((NioReader) event.value());

                        for(SelectionKey key : contexts.keySet()) {
                            ConcurrentLinkedQueue<NioWriter> queue = contexts.get(key).getWriters();

                            if(mdtpRequest.method() == MdtpMethod.SET
                                    || mdtpRequest.method() == MdtpMethod.DELETE) {
                                if(raftState.getRaftRole() == RaftRole.Leader) {
                                    /* 发送AppendEntries */
                                    queue.offer(RaftRpc.newAppendEntries(key, mdtpRequest));
                                } else if(raftState.getRaftRole() == RaftRole.Follower) {
                                    /* 发送重定向的请求 */
                                    queue.offer(RaftRpc.newRedirectMdtpRequest(key, mdtpRequest));
                                }
                            }

                            key.interestOpsOr(SelectionKey.OP_WRITE);
                        }
                        event = poll();
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
    private void requestVote() throws IOException {
        raftState.setRaftRole(RaftRole.Candidate);
        raftState.setCurrentTerm(raftState.getCurrentTerm() + 1);
        raftState.setVotedFor(new RaftNode(config.getHost(), config.getPort()));
        HashSet<RaftNode> votedNodes = raftState.getVotedNodes();

        synchronized (votedNodes) {
            votedNodes.clear();
        }

        for (SelectionKey key : contexts.keySet()) {
            RaftRpcClientContext context = contexts.get(key);
            context.getWriters().offer(RaftRpc.newRequestVote(key));
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

        public RpcClientIoEventHandler(CountDownLatch latch,
                                       SelectionKey selectionKey) {
            this.latch = latch;
            this.selectionKey = selectionKey;
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
            contexts.put(selectionKey, new RaftRpcClientContext(selectionKey));
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }

        private void doRead() throws IOException {
            RaftRpcClientContext context = contexts.get(selectionKey);
            NioReader reader = context.getReader();
            RaftNode raftNode = (RaftNode) selectionKey.attachment();

            try {
                reader.read();
            } catch (SocketException e) {
                handleDisconnect();
                e.printStackTrace();
            }

            if(reader.isReadCompleted()) {
                ResponseHandler.handle(reader, raftNode);
                context.newReader();
            }
        }

        private void doWrite() throws IOException {
            ConcurrentLinkedQueue<NioWriter> writers = contexts.get(selectionKey).getWriters();
            NioWriter writer = writers.peek();

            if(writer == null) return;
            writer.write();

            if(writer.isWriteCompleted()) {
                writers.poll();
                if(writers.isEmpty()) {
                    /* 只监听读 */
                    selectionKey.interestOpsAnd(SelectionKey.OP_READ);
                } else {
                    /* 监听读写 */
                    selectionKey.interestOpsOr(SelectionKey.OP_READ);
                }
            }
        }

        /**
         * 处理节点断开连接的情况
         */
        private void handleDisconnect() {
            failureNodes.add((RaftNode) selectionKey.attachment());
            selectionKey.cancel();
            contexts.remove(selectionKey);
            logger.info("Raft Node {} is disconnect.", selectionKey.attachment());
        }
    }
}
