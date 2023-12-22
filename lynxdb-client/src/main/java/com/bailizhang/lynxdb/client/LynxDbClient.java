/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
