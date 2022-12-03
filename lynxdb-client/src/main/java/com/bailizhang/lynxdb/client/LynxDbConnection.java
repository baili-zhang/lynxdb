package com.bailizhang.lynxdb.client;

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
        if(current == null || !current.isValid()) {
            try {
                current = socketClient.connect(serverNode);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // TODO 可能出现还没连上就发消息

        return current;
    }

    public void serverNode(ServerNode node) {
        serverNode = node;
    }
}
