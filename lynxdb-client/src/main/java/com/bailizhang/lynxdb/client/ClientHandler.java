package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements SocketClientHandler {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> futureMap;

    public ClientHandler(ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> map) {
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
    public void handleBeforeSend(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(selectionKey);
        if(map == null) {
            map = new ConcurrentHashMap<>();
            futureMap.put(selectionKey, map);
        }
        map.put(serial, new LynxDbFuture<>());
    }

    @Override
    public void handleResponse(SocketResponse response) {
        int serial = response.serial();
        SelectionKey key = response.selectionKey();
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(key);

        if(map == null) {
            throw new RuntimeException("Map is null");
        }

        LynxDbFuture<byte[]> future = map.remove(serial);
        future.value(response.data());
    }
}
