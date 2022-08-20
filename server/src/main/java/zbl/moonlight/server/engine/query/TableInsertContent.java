package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.Key;
import zbl.moonlight.storage.core.MultiTableRows;

import java.nio.ByteBuffer;
import java.util.*;


public class TableInsertContent {
    private final String table;
    private final MultiTableRows rows;

    public TableInsertContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        List<Key> keys = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        byte[] keysBytes = BufferUtils.getBytes(buffer);
        byte[] columnBytes = BufferUtils.getBytes(buffer);
        byte[] valueBytes = BufferUtils.getBytes(buffer);

        ByteBuffer keysBuffer = ByteBuffer.wrap(keysBytes);

        while(!BufferUtils.isOver(keysBuffer)) {
            byte[] bytes = BufferUtils.getBytes(keysBuffer);
            keys.add(new Key(bytes));
        }

        ByteBuffer columnBuffer = ByteBuffer.wrap(columnBytes);

        while(!BufferUtils.isOver(columnBuffer)) {
            byte[] bytes = BufferUtils.getBytes(keysBuffer);
            columns.add(new Column(bytes));
        }

        ByteBuffer valuesBuffer = ByteBuffer.wrap(valueBytes);

        while(!BufferUtils.isOver(valuesBuffer)) {
            byte[] bytes = BufferUtils.getBytes(keysBuffer);
            values.add(bytes);
        }

        rows = new MultiTableRows();

        int i = 0;
        for(Key key : keys) {
            Map<Column, byte[]> row = new HashMap<>();

            for(Column column : columns) {
                row.put(column, values.get(i ++));
            }

            rows.put(key, row);
        }
    }

    public String table() {
        return table;
    }

    public MultiTableRows rows() {
        return rows;
    }
}
