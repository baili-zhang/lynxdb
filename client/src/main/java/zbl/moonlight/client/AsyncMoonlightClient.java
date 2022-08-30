package zbl.moonlight.client;

import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.server.engine.query.*;
import zbl.moonlight.socket.client.SocketClient;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.MultiTableKeys;
import zbl.moonlight.storage.core.MultiTableRows;
import zbl.moonlight.storage.core.Pair;

import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncMoonlightClient extends SocketClient {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, MoonlightFuture>> futureMap = new ConcurrentHashMap<>();

    public AsyncMoonlightClient() {
        AsyncClientHandler handler = new AsyncClientHandler(futureMap);
        setHandler(handler);
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

    public MoonlightFuture asyncDropKvstore(SelectionKey selectionKey,
                                            List<String> kvstores) {
        MoonlightFuture future = new MoonlightFuture();
        DropKvStoreContent content = new DropKvStoreContent(kvstores);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncDropTable(SelectionKey selectionKey,
                                          List<String> tables) {
        MoonlightFuture future = new MoonlightFuture();
        DropTableContent content = new DropTableContent(tables);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncDropTableColumn(SelectionKey selectionKey,
                                                String table,
                                                HashSet<Column> columns) {
        MoonlightFuture future = new MoonlightFuture();
        DropTableColumnContent content = new DropTableColumnContent(table, columns);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncKvDelete(SelectionKey selectionKey,
                                         String kvstore,
                                         List<byte[]> keys) {
        MoonlightFuture future = new MoonlightFuture();
        KvDeleteContent content = new KvDeleteContent(kvstore, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
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

    public MoonlightFuture asyncKvSet(SelectionKey selectionKey,
                                      String kvstore,
                                      List<Pair<byte[], byte[]>> kvPairs) {
        MoonlightFuture future = new MoonlightFuture();
        KvSetContent content = new KvSetContent(kvstore, kvPairs);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncTableDelete(SelectionKey selectionKey,
                                            String table,
                                            List<byte[]> keys) {
        MoonlightFuture future = new MoonlightFuture();
        TableDeleteContent content = new TableDeleteContent(table, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncTableInsert(SelectionKey selectionKey,
                                            String table,
                                            MultiTableRows rows) {
        MoonlightFuture future = new MoonlightFuture();
        TableInsertContent content = new TableInsertContent(table, rows);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public MoonlightFuture asyncTableSelect(SelectionKey selectionKey,
                                            String table,
                                            MultiTableKeys keys) {
        MoonlightFuture future = new MoonlightFuture();
        TableSelectContent content = new TableSelectContent(table, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }
}
