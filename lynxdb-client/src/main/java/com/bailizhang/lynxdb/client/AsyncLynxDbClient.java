package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.query.*;
import com.bailizhang.lynxdb.socket.client.ClientRequest;
import com.bailizhang.lynxdb.socket.client.SocketClient;
import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.MultiTableKeys;
import com.bailizhang.lynxdb.storage.core.MultiTableRows;
import com.bailizhang.lynxdb.storage.core.Pair;

import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncLynxDbClient extends SocketClient {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture>> futureMap = new ConcurrentHashMap<>();

    public AsyncLynxDbClient() {
        AsyncClientHandler handler = new AsyncClientHandler(futureMap);
        setHandler(handler);
    }

    public LynxDbFuture asyncCreateTable(SelectionKey selectionKey,
                                         List<String> tables) {
        LynxDbFuture future = new LynxDbFuture();
        CreateTableContent content = new CreateTableContent(tables);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncCreateKvstore(SelectionKey selectionKey,
                                           List<String> kvstores) {
        LynxDbFuture future = new LynxDbFuture();
        CreateKvStoreContent content = new CreateKvStoreContent(kvstores);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncCreateTableColumnByBytesList(SelectionKey selectionKey,
                                                          String table,
                                                          List<byte[]> columns) {
        LynxDbFuture future = new LynxDbFuture();
        CreateTableColumnContent content = new CreateTableColumnContent(table, columns);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncCreateTableColumnByStrList(SelectionKey selectionKey,
                                                        String table,
                                                        List<String> columns) {
        List<byte[]> cols = columns.stream().map(G.I::toBytes).toList();
        return asyncCreateTableColumnByBytesList(selectionKey, table, cols);
    }

    public LynxDbFuture asyncDropKvstore(SelectionKey selectionKey,
                                         List<String> kvstores) {
        LynxDbFuture future = new LynxDbFuture();
        DropKvStoreContent content = new DropKvStoreContent(kvstores);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncDropTable(SelectionKey selectionKey,
                                       List<String> tables) {
        LynxDbFuture future = new LynxDbFuture();
        DropTableContent content = new DropTableContent(tables);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncDropTableColumn(SelectionKey selectionKey,
                                             String table,
                                             HashSet<Column> columns) {
        LynxDbFuture future = new LynxDbFuture();
        DropTableColumnContent content = new DropTableColumnContent(table, columns);

        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncKvDeleteByBytesList(SelectionKey selectionKey,
                                                 String kvstore,
                                                 List<byte[]> keys) {
        LynxDbFuture future = new LynxDbFuture();
        KvDeleteContent content = new KvDeleteContent(kvstore, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncKvDelete1(SelectionKey selectionKey,
                                       String kvstore,
                                       List<String> keys) {
        List<byte[]> keyList = keys.stream().map(G.I::toBytes).toList();
        return asyncKvDeleteByBytesList(selectionKey, kvstore, keyList);
    }

    public LynxDbFuture asyncKvGetByBytesList(SelectionKey selectionKey,
                                              String kvstore, List<byte[]> keys) {
        LynxDbFuture future = new LynxDbFuture();
        KvGetContent content = new KvGetContent(kvstore, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncKvGetByStrList(SelectionKey selectionKey,
                                            String kvstore, List<String> keys) {
        List<byte[]> keyList = keys.stream().map(G.I::toBytes).toList();
        return asyncKvGetByBytesList(selectionKey, kvstore, keyList);
    }

    public LynxDbFuture asyncKvSet(SelectionKey selectionKey,
                                   String kvstore,
                                   List<Pair<byte[], byte[]>> kvPairs) {
        LynxDbFuture future = new LynxDbFuture();
        KvSetContent content = new KvSetContent(kvstore, kvPairs);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncTableDelete0(SelectionKey selectionKey,
                                         String table,
                                         List<byte[]> keys) {
        LynxDbFuture future = new LynxDbFuture();
        TableDeleteContent content = new TableDeleteContent(table, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncTableDelete1(SelectionKey selectionKey,
                                          String table,
                                          List<String> keys) {
        List<byte[]> keyList = keys.stream().map(G.I::toBytes).toList();
        return asyncTableDelete0(selectionKey, table, keyList);
    }

    public LynxDbFuture asyncTableInsert(SelectionKey selectionKey,
                                         String table,
                                         MultiTableRows rows) {
        LynxDbFuture future = new LynxDbFuture();
        TableInsertContent content = new TableInsertContent(table, rows);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncTableSelect(SelectionKey selectionKey,
                                         String table,
                                         MultiTableKeys keys) {
        LynxDbFuture future = new LynxDbFuture();
        TableSelectContent content = new TableSelectContent(table, keys);
        ClientRequest request = new ClientRequest(selectionKey, content);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncShowTable(SelectionKey selectionKey) {
        LynxDbFuture future = new LynxDbFuture();
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(LdtpMethod.SHOW_TABLE);
        ClientRequest request = new ClientRequest(selectionKey, bytesList);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncShowKvstore(SelectionKey selectionKey) {
        LynxDbFuture future = new LynxDbFuture();
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(LdtpMethod.SHOW_KVSTORE);
        ClientRequest request = new ClientRequest(selectionKey, bytesList);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }

    public LynxDbFuture asyncShowTableColumn(SelectionKey selectionKey,
                                             String table) {
        LynxDbFuture future = new LynxDbFuture();
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(LdtpMethod.SHOW_TABLE_COLUMN);
        bytesList.appendRawStr(table);
        ClientRequest request = new ClientRequest(selectionKey, bytesList);

        int serial = send(request);
        futureMap.get(selectionKey).put(serial, future);

        return future;
    }
}
