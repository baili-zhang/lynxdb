package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.MultiTableKeys;
import com.bailizhang.lynxdb.storage.core.MultiTableRows;
import com.bailizhang.lynxdb.storage.core.Pair;

import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.List;

public class LynxDbClient extends AsyncLynxDbClient {
    public LynxDbClient() {
    }

    public byte[] createTable(SelectionKey selectionKey,
                              List<String> tables) {
        LynxDbFuture future = asyncCreateTable(selectionKey, tables);
        return future.get();
    }

    public byte[] createKvstore(SelectionKey selectionKey,
                                List<String> kvstores) {
        LynxDbFuture future = asyncCreateKvstore(selectionKey, kvstores);
        return future.get();
    }

    public byte[] createTableColumn0(SelectionKey selectionKey,
                                     String table,
                                     List<byte[]> columns) {
        LynxDbFuture future = asyncCreateTableColumn0(selectionKey, table, columns);
        return future.get();
    }

    public byte[] createTableColumn1(SelectionKey selectionKey,
                                     String table,
                                     List<String> columns) {
        LynxDbFuture future = asyncCreateTableColumn1(selectionKey, table, columns);
        return future.get();
    }

    public byte[] dropKvstore(SelectionKey selectionKey,
                              List<String> kvstores) {
        LynxDbFuture future = asyncDropKvstore(selectionKey, kvstores);
        return future.get();
    }

    public byte[] dropTable(SelectionKey selectionKey,
                                  List<String> tables) {
        LynxDbFuture future = asyncDropTable(selectionKey, tables);
        return future.get();
    }

    public byte[] dropTableColumn(SelectionKey selectionKey,
                                  String table,
                                  HashSet<Column> columns) {
        LynxDbFuture future = asyncDropTableColumn(selectionKey, table, columns);
        return future.get();
    }

    public byte[] kvDelete0(SelectionKey selectionKey,
                                       String kvstore,
                                       List<byte[]> keys) {
        LynxDbFuture future = asyncKvDelete0(selectionKey, kvstore, keys);
        return future.get();
    }

    public byte[] kvDelete1(SelectionKey selectionKey,
                            String kvstore,
                            List<String> keys) {
        LynxDbFuture future = asyncKvDelete1(selectionKey, kvstore, keys);
        return future.get();
    }

    public byte[] kvGet0(SelectionKey selectionKey,
                         String kvstore, List<byte[]> keys) {
        LynxDbFuture future = asyncKvGet0(selectionKey, kvstore, keys);
        return future.get();
    }

    public byte[] kvGet1(SelectionKey selectionKey,
                         String kvstore, List<String> keys) {
        LynxDbFuture future = asyncKvGet1(selectionKey, kvstore, keys);
        return future.get();
    }

    public byte[] kvSet(SelectionKey selectionKey,
                        String kvstore,
                        List<Pair<byte[], byte[]>> kvPairs) {
        LynxDbFuture future = asyncKvSet(selectionKey, kvstore, kvPairs);
        return future.get();
    }

    public byte[] tableDelete0(SelectionKey selectionKey,
                                    String table,
                                    List<byte[]> keys) {
        LynxDbFuture future = asyncTableDelete0(selectionKey, table, keys);
        return future.get();
    }

    public byte[] tableDelete1(SelectionKey selectionKey,
                                     String table,
                                     List<String> keys) {
        LynxDbFuture future = asyncTableDelete1(selectionKey, table, keys);
        return future.get();
    }

    public byte[] tableInsert(SelectionKey selectionKey,
                                         String table,
                                         MultiTableRows rows) {
        LynxDbFuture future = asyncTableInsert(selectionKey, table, rows);
        return future.get();
    }

    public byte[] tableSelect(SelectionKey selectionKey,
                                         String table,
                                         MultiTableKeys keys) {
        LynxDbFuture future = asyncTableSelect(selectionKey, table, keys);
        return future.get();
    }

    public byte[] showTable(SelectionKey selectionKey) {
        LynxDbFuture future = asyncShowTable(selectionKey);
        return future.get();
    }

    public byte[] showKvstore(SelectionKey selectionKey) {
        LynxDbFuture future = asyncShowKvstore(selectionKey);
        return future.get();
    }

    public byte[] showTableColumn(SelectionKey selectionKey,
                                        String table) {
        LynxDbFuture future = asyncShowTableColumn(selectionKey, table);
        return future.get();
    }
}
