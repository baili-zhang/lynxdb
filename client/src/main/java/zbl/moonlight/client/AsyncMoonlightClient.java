package zbl.moonlight.client;

import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.server.engine.query.KvGetContent;
import zbl.moonlight.socket.client.SocketClient;

import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncMoonlightClient extends SocketClient {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, MoonlightFuture>> futureMap = new ConcurrentHashMap<>();

    public AsyncMoonlightClient() {
        AsyncClientHandler handler = new AsyncClientHandler(futureMap);
        setHandler(handler);
    }

    public MoonlightFuture asyncKvGet(SelectionKey selectionKey,
                                      String kvstore, List<byte[]> keys) {
        MoonlightFuture future = new MoonlightFuture();
        KvGetContent content = new KvGetContent(kvstore, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }
}
