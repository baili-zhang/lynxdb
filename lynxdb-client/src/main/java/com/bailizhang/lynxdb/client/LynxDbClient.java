package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.client.exception.LynxDbException;
import com.bailizhang.lynxdb.client.utils.LynxDbUtils;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.MultiTableKeys;
import com.bailizhang.lynxdb.storage.core.MultiTableRows;
import com.bailizhang.lynxdb.storage.core.Pair;

import java.lang.reflect.Field;
import java.nio.channels.SelectionKey;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class LynxDbClient extends AsyncLynxDbClient {
    public LynxDbClient() {
    }

    public void createTable(SelectionKey selectionKey,
                            List<String> tables) {
        LynxDbFuture future = asyncCreateTable(selectionKey, tables);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void createKvstore(SelectionKey selectionKey,
                              List<String> kvstores) {
        LynxDbFuture future = asyncCreateKvstore(selectionKey, kvstores);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void createTableColumnByBytesList(SelectionKey selectionKey,
                                   String table,
                                   List<byte[]> columns) {
        LynxDbFuture future = asyncCreateTableColumnByBytesList(selectionKey, table, columns);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void createTableColumnByStrList(SelectionKey selectionKey,
                                   String table,
                                   List<String> columns) {
        LynxDbFuture future = asyncCreateTableColumn(selectionKey, table, columns);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void dropKvstore(SelectionKey selectionKey,
                            List<String> kvstores) {
        LynxDbFuture future = asyncDropKvstore(selectionKey, kvstores);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void dropTable(SelectionKey selectionKey,
                          List<String> tables) {
        LynxDbFuture future = asyncDropTable(selectionKey, tables);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void dropTableColumn(SelectionKey selectionKey,
                                String table,
                                HashSet<Column> columns) {
        LynxDbFuture future = asyncDropTableColumn(selectionKey, table, columns);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void kvDeleteByBytesList(SelectionKey selectionKey,
                          String kvstore,
                          List<byte[]> keys) {
        LynxDbFuture future = asyncKvDeleteByBytesList(selectionKey, kvstore, keys);
        byte[] value = future.get();

        LynxDbResult result = new LynxDbResult(value);
        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
    }

    public void kvDeleteByStrList(SelectionKey selectionKey,
                          String kvstore,
                          List<String> keys) {
        List<byte[]> keyList = keys.stream().map(G.I::toBytes).toList();
        kvDeleteByBytesList(selectionKey, kvstore, keyList);
    }

    public byte[] kvGetByBytesList(SelectionKey selectionKey,
                         String kvstore, List<byte[]> keys) {
        LynxDbFuture future = asyncKvGetByBytesList(selectionKey, kvstore, keys);
        return future.get();
    }

    public byte[] kvGet(SelectionKey selectionKey,
                        String kvstore, List<String> keys) {
        LynxDbFuture future = asyncKvGet(selectionKey, kvstore, keys);
        return future.get();
    }

    public <T> T kvGet(SelectionKey selectionKey, Class<T> clazz) {
        String kvstore = LynxDbUtils.findKvstoreName(clazz);

        Collection<Field> fields = LynxDbUtils.findFields(clazz);
        List<String> keys = FieldUtils.findNames(fields);

        byte[] value = kvGet(selectionKey, kvstore, keys);
        LynxDbResult result = new LynxDbResult(value);
        return result.kvGet(clazz);
    }

    public <T> T kvGetSelective(SelectionKey selectionKey, List<String> keys, Class<T> clazz) {
        String kvstore = LynxDbUtils.findKvstoreName(clazz);
        byte[] value = kvGet(selectionKey, kvstore, keys);
        LynxDbResult result = new LynxDbResult(value);
        return result.kvGet(clazz);
    }

    public byte[] kvSet(SelectionKey selectionKey,
                        String kvstore,
                        List<Pair<byte[], byte[]>> kvPairs) {
        LynxDbFuture future = asyncKvSet(selectionKey, kvstore, kvPairs);
        return future.get();
    }

    public void kvSet(SelectionKey selectionKey, Object o) {
        Class<?> clazz = o.getClass();
        String kvstore = LynxDbUtils.findKvstoreName(clazz);
        List<Pair<byte[], byte[]>> kvPairs = LynxDbUtils.findKvPairs(o);
        byte[] value = kvSet(selectionKey, kvstore, kvPairs);
        LynxDbResult result = new LynxDbResult(value);

        if(result.isSuccessful()) {
            return;
        }
        throw new LynxDbException(result.message());
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
