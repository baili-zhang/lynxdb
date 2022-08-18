package zbl.moonlight.server.engine;

import zbl.moonlight.core.common.G;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.engine.query.*;
import zbl.moonlight.server.engine.result.Result;
import zbl.moonlight.storage.core.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;
import static zbl.moonlight.server.annotations.MdtpMethod.*;

public class MdtpStorageEngine extends BaseStorageEngine {
    private static final String KEY = "Key";
    private static final String KVSTORES = "KV Stores";
    private static final String TABLES = "Tables";

    public MdtpStorageEngine() {
        super(MdtpStorageEngine.class);
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

    @MdtpMethod(CREATE_KV_STORE)
    public byte[] doCreateKvStore(QueryParams params) {
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

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(DROP_KV_STORE)
    public byte[] doDropKvStore(QueryParams params) {
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

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(KV_SET)
    public byte[] doKvSet(QueryParams params) {
        KvSetContent content = new KvSetContent(params);

        KvAdapter db = kvDbMap.get(content.kvstore());
        db.set(content.kvPairs());

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(KV_GET)
    public byte[] doKvGet(QueryParams params) {
        KvGetContent content = new KvGetContent(params);

        String kvstore = content.kvstore();
        KvAdapter db = kvDbMap.get(kvstore);

        if(db == null) {
            String template = "Kvstore \"%s\" is not existed.";
            String errorMsg = String.format(template, kvstore);
            return Result.invalidArgument(errorMsg);
        }

        List<Pair<byte[], byte[]>> values = db.get(content.keys());

        AtomicInteger length = new AtomicInteger(0);
        values.forEach(pair -> length.getAndAdd(pair.left().length + pair.right().length + 2 * INT_LENGTH));

        ByteBuffer buffer = ByteBuffer.allocate(length.get() + BYTE_LENGTH);
        buffer.put(Result.SUCCESS);

        values.forEach(pair -> {
            int keyLen = pair.left().length;
            int valLen = pair.right().length;

            buffer.putInt(keyLen).put(pair.left())
                    .putInt(valLen).put(pair.right());
        });

        return buffer.array();
    }

    @MdtpMethod(KV_DELETE)
    public byte[] doKvDelete(QueryParams params) {
        KvDeleteContent content = new KvDeleteContent(params);

        KvAdapter db = kvDbMap.get(content.kvstore());
        db.delete(content.keys());

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(CREATE_TABLE)
    public byte[] doCreateTable(QueryParams params) {
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

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(DROP_TABLE)
    public byte[] doDropTable(QueryParams params) {
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

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(CREATE_TABLE_COLUMN)
    public byte[] doCreateTableColumn(QueryParams params) {
        CreateTableColumnContent content = new CreateTableColumnContent(params);
        List<byte[]> columns = content.columns();

        TableAdapter db = tableMap.get(content.table());

        for(byte[] bytes : columns) {
            Column column = new Column(bytes);

            if(db.columns().contains(column)) {
                String template = "Table column \"%s\" has existed.";
                String errorMsg = String.format(template, column);
                return Result.invalidArgument(errorMsg);
            }
        }

        db.createColumns(columns);

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(DROP_TABLE_COLUMN)
    public byte[] doDropTableColumn(QueryParams params) {
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

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(TABLE_GET)
    public byte[] doTableGet(QueryParams params) {
        TableGetContent content = new TableGetContent(params);

        TableAdapter db = tableMap.get(content.table());
        MultiTableRows rows = db.get(content.multiKeys());

        List<byte[]> keys = content.keys();
        List<byte[]> columns = content.columns().stream().map(Column::value).toList();

        byte[] keysBytes = BufferUtils.toBytes(keys);
        byte[] columnsBytes = BufferUtils.toBytes(columns);

        List<byte[]> values = new ArrayList<>();

        for(byte[] key : keys) {
            Map<Column, byte[]> row = rows.get(new Key(key));

            for(byte[] column : columns) {
                byte[] value = row.get(new Column(column));
                values.add(value);
            }
        }

        byte[] valuesBytes = BufferUtils.toBytes(values);

        List<byte[]> total = new ArrayList<>();
        total.add(keysBytes);
        total.add(columnsBytes);
        total.add(valuesBytes);

        byte[] totalBytes = BufferUtils.toBytes(total);

        ByteBuffer buffer = ByteBuffer.allocate(BYTE_LENGTH + totalBytes.length);

        return buffer.put(Result.SUCCESS).put(totalBytes).array();
    }

    @MdtpMethod(TABLE_SET)
    public byte[] doTableSet(QueryParams params) {
        TableSetContent content = new TableSetContent(params);
        TableAdapter db = tableMap.get(content.table());

        db.set(content.rows());

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(TABLE_DELETE)
    public byte[] doTableDelete(QueryParams params) {
        TableDeleteContent content = new TableDeleteContent(params);
        TableAdapter db = tableMap.get(content.table());

        db.delete(content.keys());

        return new byte[]{Result.SUCCESS};
    }

    @MdtpMethod(SHOW_KVSTORE)
    public byte[] doShowKvstore(QueryParams params) {
        List<byte[]> total = new ArrayList<>();

        total.add(G.I.toBytes(KVSTORES));

        for (String kvstore : kvDbMap.keySet()) {
            total.add(G.I.toBytes(kvstore));
        }

        byte[] totalBytes = BufferUtils.toBytes(total);

        ByteBuffer buffer = ByteBuffer.allocate(BYTE_LENGTH + totalBytes.length);

        return buffer.put(Result.SUCCESS_SHOW).put(totalBytes).array();
    }

    @MdtpMethod(SHOW_TABLE)
    public byte[] doShowTable(QueryParams params) {
        List<byte[]> total = new ArrayList<>();

        total.add(G.I.toBytes(TABLES));

        for (String table : tableMap.keySet()) {
            total.add(G.I.toBytes(table));
        }

        byte[] totalBytes = BufferUtils.toBytes(total);

        ByteBuffer buffer = ByteBuffer.allocate(BYTE_LENGTH + totalBytes.length);

        return buffer.put(Result.SUCCESS_SHOW).put(totalBytes).array();
    }
}
