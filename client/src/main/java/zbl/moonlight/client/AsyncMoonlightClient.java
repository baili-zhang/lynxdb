package zbl.moonlight.client;

import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.server.engine.query.CreateKvStoreContent;
import zbl.moonlight.server.engine.query.CreateTableColumnContent;
import zbl.moonlight.server.engine.query.CreateTableContent;
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

    public MoonlightFuture asyncCreateTable(SelectionKey selectionKey,
                                            List<String> tables) {
        MoonlightFuture future = new MoonlightFuture();
        CreateTableContent content = new CreateTableContent(tables);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncCreateKvstore(SelectionKey selectionKey,
                                              List<String> kvstores) {
        MoonlightFuture future = new MoonlightFuture();
        CreateKvStoreContent content = new CreateKvStoreContent(kvstores);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncCreateTableColumn(SelectionKey selectionKey,
                                                  String table,
                                                  List<byte[]> columns) {
        MoonlightFuture future = new MoonlightFuture();
        CreateTableColumnContent content = new CreateTableColumnContent(table, columns);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }
}
