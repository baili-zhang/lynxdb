package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class LynxDbConnection {
    private final SocketClient socketClient;

    private SelectionKey current;
    private ServerNode serverNode;

    public LynxDbConnection(SocketClient client) {
        socketClient = client;
    }

    public SelectionKey current() {
        if(current != null && current.isValid()) {
            return current;
        }

        try {
            LynxDbFuture<SelectionKey> future = socketClient.connect(serverNode);
            current = future.get();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return current;
    }

    public void serverNode(ServerNode node) {
        serverNode = node;
    }
}
