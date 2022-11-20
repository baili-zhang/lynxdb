package com.bailizhang.lynxdb.client.async;

import com.bailizhang.lynxdb.client.LynxDbFuture;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncClientHandler implements SocketClientHandler {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture>> futureMap;

    public AsyncClientHandler(ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture>> map) {
        futureMap = map;
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) {
        futureMap.put(selectionKey, new ConcurrentHashMap<>());
    }

    @Override
    public void handleDisconnect(SelectionKey selectionKey) {
        futureMap.remove(selectionKey);
    }

    @Override
    public void handleResponse(SocketResponse response) {
        int serial = response.serial();
        SelectionKey key = response.selectionKey();
        ConcurrentHashMap<Integer, LynxDbFuture> map = futureMap.get(key);

        if(map == null) {
            throw new RuntimeException("Map is null");
        }

        LynxDbFuture future = map.remove(serial);
        future.value(response.data());
    }
}
