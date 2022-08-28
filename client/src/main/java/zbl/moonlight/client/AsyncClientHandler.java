package zbl.moonlight.client;

import zbl.moonlight.socket.interfaces.SocketClientHandler;
import zbl.moonlight.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncClientHandler implements SocketClientHandler {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, MoonlightFuture>> futureMap;

    public AsyncClientHandler(ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, MoonlightFuture>> map) {
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
        ConcurrentHashMap<Integer, MoonlightFuture> map = futureMap.get(key);

        if(map == null) {
            throw new RuntimeException("Map is null");
        }

        MoonlightFuture future = map.get(serial);
        future.value(response.data());
    }
}
