package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.client.AsyncLynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.MultiTableKeys;
import com.bailizhang.lynxdb.storage.core.MultiTableRows;
import com.bailizhang.lynxdb.storage.core.Pair;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.List;

public class LynxDbTemplate {
    private final AsyncLynxDbClient client;
    private final SelectionKey current;

    public LynxDbTemplate(LynxDbProperties properties) {
        client = new AsyncLynxDbClient();
        Executor.start(client);
        ServerNode server = new ServerNode(properties.getHost(), properties.getPort());

        try {
            current = client.connect(server);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public LynxDbFuture asyncCreateTable(List<String> tables) {
        return client.asyncCreateTable(current, tables);
    }

    public LynxDbFuture asyncCreateKvstore(List<String> kvstores) {
        return client.asyncCreateKvstore(current, kvstores);
    }

    public LynxDbFuture asyncCreateTableColumn(String table, List<byte[]> columns) {
        return client.asyncCreateTableColumn(current, table, columns);
    }

    public LynxDbFuture asyncDropKvstore(List<String> kvstores) {
        return client.asyncDropKvstore(current, kvstores);
    }

    public LynxDbFuture asyncDropTable(List<String> tables) {
        return client.asyncDropTable(current, tables);
    }

    public LynxDbFuture asyncDropTableColumn(String table, HashSet<Column> columns) {
        return client.asyncDropTableColumn(current, table, columns);
    }

    public LynxDbFuture asyncKvDelete(String kvstore, List<byte[]> keys) {
        return client.asyncKvDelete(current, kvstore, keys);
    }

    public LynxDbFuture asyncKvGet(String kvstore, List<byte[]> keys) {
        return client.asyncKvGet(current, kvstore, keys);
    }

    public LynxDbFuture asyncKvSet(String kvstore, List<Pair<byte[], byte[]>> kvPairs) {
        return client.asyncKvSet(current, kvstore, kvPairs);
    }

    public LynxDbFuture asyncTableDelete(String table, List<byte[]> keys) {
        return client.asyncTableDelete(current, table, keys);
    }

    public LynxDbFuture asyncTableInsert(String table, MultiTableRows rows) {
        return client.asyncTableInsert(current, table, rows);
    }

    public LynxDbFuture asyncTableSelect(String table, MultiTableKeys keys) {
        return client.asyncTableSelect(current, table, keys);
    }

    public LynxDbFuture asyncShowTable() {
        return client.asyncShowTable(current);
    }

    public LynxDbFuture asyncShowKvstore() {
        return client.asyncShowKvstore(current);
    }

    public LynxDbFuture asyncShowTableColumn(String table) {
        return client.asyncShowTableColumn(current, table);
    }
}
