package com.bailizhang.lynxdb.client.async;

import com.bailizhang.lynxdb.client.LynxDbFuture;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncLynxDbClient extends SocketClient {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture>> futureMap = new ConcurrentHashMap<>();

    public AsyncLynxDbClient() {
        AsyncClientHandler handler = new AsyncClientHandler(futureMap);
        setHandler(handler);
    }


}
