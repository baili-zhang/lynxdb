package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.client.message.MessageHandler;
import com.bailizhang.lynxdb.client.message.MessageReceiver;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbClient implements AutoCloseable {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> futureMap = new ConcurrentHashMap<>();

    private final HashMap<ServerNode, LynxDbConnection> connections = new HashMap<>();
    private final SocketClient socketClient;

    private final MessageReceiver messageReceiver;

    public LynxDbClient() {
        messageReceiver = new MessageReceiver();

        ClientHandler handler = new ClientHandler(futureMap, messageReceiver);

        socketClient = new SocketClient();
        socketClient.setHandler(handler);
    }

    public void start() {
        Executor.start(socketClient);
        Executor.start(messageReceiver);
    }

    public LynxDbConnection createConnection(String host, int port) throws ConnectException {
        ServerNode serverNode = new ServerNode(host, port);
        return createConnection(serverNode);
    }

    public LynxDbConnection createConnection(ServerNode serverNode) {
        return connections.computeIfAbsent(
                serverNode,
                node -> new LynxDbConnection(node, socketClient, futureMap)
        );
    }

    public void disconnect(String host, int port) {
        ServerNode serverNode = new ServerNode(host, port);

        LynxDbConnection connection = connections.get(serverNode);
        connection.disconnect();
    }

    public void registerAffectHandler(MessageKey messageKey, MessageHandler messageHandler) {
        messageReceiver.registerAffectHandler(messageKey, messageHandler);
    }

    public void registerTimeoutHandler(MessageKey messageKey, MessageHandler messageHandler) {
        messageReceiver.registerTimeoutHandler(messageKey, messageHandler);
    }

    @Override
    public void close() {
        socketClient.close();
        messageReceiver.shutdown();
    }
}
