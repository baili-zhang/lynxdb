package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.query.*;
import com.bailizhang.lynxdb.server.engine.result.Result;
import com.bailizhang.lynxdb.storage.core.*;
import com.bailizhang.lynxdb.storage.core.exception.ColumnsNotExistedException;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.bailizhang.lynxdb.server.annotations.LdtpMethod.*;

public class LdtpStorageEngine extends BaseStorageEngine {
    public LdtpStorageEngine() {
        super(LdtpStorageEngine.class);
    }

    public byte[] metaGet(String key) {
        return metaDb.get(key.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 为什么不用注解的方式？
     *  因为不是 Socket 通信的命令
     *  需要生成 byte[] 的 command
     *  再解析 command
     *  处理流程绕了一大圈
     *
     * @param key key
     * @param value value
     */
    public void metaSet(String key, byte[] value) {
        metaDb.set(new Pair<>(key.getBytes(StandardCharsets.UTF_8), value));
    }

    @LdtpMethod(CREATE_KV_STORE)
    public BytesList doCreateKvStore(QueryParams params) {
        CreateKvStoreContent content = new CreateKvStoreContent(params);
        List<String> kvstores = content.kvstores();

        for(String kvstore : kvstores) {
            if(kvDbMap.containsKey(kvstore)) {
                String template = "Kvstore \"%s\" has existed.";
                String errorMsg = String.format(template, kvstore);
                return Result.invalidArgument(errorMsg);
            }
        }

        content.kvstores().forEach(this::createKvDb);

        return Result.success();
    }

    @LdtpMethod(DROP_KV_STORE)
    public BytesList doDropKvStore(QueryParams params) {
        DropKvStoreContent content = new DropKvStoreContent(params);
        List<String> kvstores = content.kvstores();

        for(String kvstore : kvstores) {
            if(!kvDbMap.containsKey(kvstore)) {
                String template = "Kvstore \"%s\" is not existed.";
                String errorMsg = String.format(template, kvstore);
                return Result.invalidArgument(errorMsg);
            }
        }

        kvstores.forEach(this::dropKvDb);

        return Result.success();
    }

    @LdtpMethod(KV_SET)
    public BytesList doKvSet(QueryParams params) {
        KvSetContent content = new KvSetContent(params);

        KvAdapter db = kvDbMap.get(content.kvstore());
        db.set(content.kvPairs());

        return Result.success();
    }

    @LdtpMethod(KV_GET)
    public BytesList doKvGet(QueryParams params) {
        KvGetContent content = new KvGetContent(params);

        String kvstore = content.kvstore();
        KvAdapter db = kvDbMap.get(kvstore);

        if(db == null) {
            String template = "Kvstore \"%s\" is not existed.";
            String errorMsg = String.format(template, kvstore);
            return Result.invalidArgument(errorMsg);
        }

        List<Pair<byte[], byte[]>> values = db.get(content.keys());

        List<byte[]> total = new ArrayList<>();

        values.forEach(pair -> {
            total.add(pair.left());
            total.add(pair.right() == null ? BufferUtils.EMPTY_BYTES : pair.right());
        });

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(Result.SUCCESS_WITH_KV_PAIRS);
        total.forEach(bytesList::appendVarBytes);

        return bytesList;
    }

    @LdtpMethod(KV_DELETE)
    public BytesList doKvDelete(QueryParams params) {
        KvDeleteContent content = new KvDeleteContent(params);

        KvAdapter db = kvDbMap.get(content.kvstore());
        db.delete(content.keys());

        return Result.success();
    }

    @LdtpMethod(CREATE_TABLE)
    public BytesList doCreateTable(QueryParams params) {
        CreateTableContent content = new CreateTableContent(params);
        List<String> tables = content.tables();

        for(String table : tables) {
            if(tableMap.containsKey(table)) {
                String template = "Table \"%s\" has existed.";
                String errorMsg = String.format(template, table);
                return Result.invalidArgument(errorMsg);
            }
        }

        tables.forEach(this::createTableDb);

        return Result.success();
    }

    @LdtpMethod(DROP_TABLE)
    public BytesList doDropTable(QueryParams params) {
        DropTableContent content = new DropTableContent(params);
        List<String> tables = content.tables();

        for(String table : tables) {
            if(!tableMap.containsKey(table)) {
                String template = "Table \"%s\" is not existed.";
                String errorMsg = String.format(template, table);
                return Result.invalidArgument(errorMsg);
            }
        }

        tables.forEach(this::dropTableDb);

        return Result.success();
    }

    @LdtpMethod(CREATE_TABLE_COLUMN)
    public BytesList doCreateTableColumn(QueryParams params) {
        CreateTableColumnContent content = new CreateTableColumnContent(params);
        List<byte[]> columns = content.columns();

        TableAdapter db = tableMap.get(content.table());

        List<String> alreadyExisted = new ArrayList<>();
        for(byte[] bytes : columns) {
            Column column = new Column(bytes);

            if(db.columns().contains(column)) {
                alreadyExisted.add(new String(column.value()));
            }
        }

        if(!alreadyExisted.isEmpty()) {
            String template = "Table column %s has existed.";
            String columnsMsg = String.join(", ",
                    alreadyExisted
                            .stream()
                            .map(s -> "\"" + s + "\"")
                            .toList()
            ) ;

            String errorMsg = String.format(template, columnsMsg);
            return Result.invalidArgument(errorMsg);
        }

        db.createColumns(columns);

        return Result.success();
    }

    @LdtpMethod(DROP_TABLE_COLUMN)
    public BytesList doDropTableColumn(QueryParams params) {
        DropTableColumnContent content = new DropTableColumnContent(params);
        HashSet<Column> columns = content.columns();

        TableAdapter db = tableMap.get(content.table());

        for(Column column : columns) {
            if(!db.columns().contains(column)) {
                String template = "Table column \"%s\" is not existed.";
                String errorMsg = String.format(template, column);
                return Result.invalidArgument(errorMsg);
            }
        }

        db.dropColumns(columns);

        return Result.success();
    }

    @LdtpMethod(TABLE_SELECT)
    public BytesList doTableSelect(QueryParams params) {
        TableSelectContent content = new TableSelectContent(params);

        TableAdapter db = tableMap.get(content.table());

        if(db == null) {
            String template = "Table \"%s\" is not existed.";
            String message = String.format(template, content.table());
            return Result.invalidArgument(message);
        }

        MultiTableRows rows = db.get(content.multiKeys());

        List<byte[]> keys = content.keys();
        List<byte[]> columns = content.columns().stream().map(Column::value).toList();

        List<byte[]> table = new ArrayList<>(columns);

        int columnSize = table.size();

        for(byte[] key : keys) {
            Map<Column, byte[]> row = rows.get(new Key(key));

            table.add(key);

            for(byte[] column : columns) {
                byte[] value = row.get(new Column(column));
                table.add(value == null ? BufferUtils.EMPTY_BYTES : value);
            }
        }

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(Result.SUCCESS_WITH_TABLE);
        bytesList.appendRawInt(columnSize);
        table.forEach(bytesList::appendVarBytes);

        return bytesList;
    }

    @LdtpMethod(TABLE_INSERT)
    public BytesList doTableInsert(QueryParams params) {
        TableInsertContent content = new TableInsertContent(params);
        TableAdapter db = tableMap.get(content.table());

        try {
            db.set(content.rows());
        } catch (ColumnsNotExistedException e) {
            String template = "Table columns %s is not existed.";

            String columns = String.join(
                    ", ",
                    e.columns()
                            .stream()
                            .map(s -> "\"" + s + "\"")
                            .toList()
            );

            String errorMsg = String.format(template, columns);
            return Result.invalidArgument(errorMsg);
        }

        return Result.success();
    }

    @LdtpMethod(TABLE_DELETE)
    public BytesList doTableDelete(QueryParams params) {
        TableDeleteContent content = new TableDeleteContent(params);
        TableAdapter db = tableMap.get(content.table());

        db.delete(content.keys());

        return Result.success();
    }

    @LdtpMethod(SHOW_KVSTORE)
    public BytesList doShowKvstore(QueryParams params) {
        List<byte[]> total = new ArrayList<>();

        for (String kvstore : kvDbMap.keySet()) {
            total.add(G.I.toBytes(kvstore));
        }

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(Result.SUCCESS_WITH_LIST);
        total.forEach(bytesList::appendVarBytes);

        return bytesList;
    }

    @LdtpMethod(SHOW_TABLE)
    public BytesList doShowTable(QueryParams params) {
        List<byte[]> total = new ArrayList<>();

        for (String table : tableMap.keySet()) {
            total.add(G.I.toBytes(table));
        }

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(Result.SUCCESS_WITH_LIST);
        total.forEach(bytesList::appendVarBytes);

        return bytesList;
    }

    @LdtpMethod(SHOW_TABLE_COLUMN)
    public BytesList doShowTableColumn(QueryParams params) {
        String table = new String(params.content());

        TableAdapter db = tableMap.get(table);

        if(db == null) {
            String template = "Table \"%s\" is not existed.";
            String message = String.format(template, table);
            return Result.invalidArgument(message);
        }

        List<byte[]> columns = db.columns().stream()
                .map(Column::value)
                .filter(val -> !Arrays.equals(val, RocksDB.DEFAULT_COLUMN_FAMILY))
                .toList();


        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(Result.SUCCESS_WITH_LIST);
        columns.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
