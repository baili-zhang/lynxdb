package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbClient implements AutoCloseable {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> futureMap = new ConcurrentHashMap<>();

    private final HashMap<ServerNode, LynxDbConnection> connections = new HashMap<>();
    private final SocketClient socketClient;

    public LynxDbClient() {
        ClientHandler handler = new ClientHandler(futureMap);

        socketClient = new SocketClient();
        socketClient.setHandler(handler);
    }

    public void start() {
        Executor.start(socketClient);
    }

    public LynxDbConnection createConnection(String host, int port) {
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

    @Override
    public void close() {
        socketClient.close();
    }
}
