package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.MultiTableKeys;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class TableGetContent {
    private final String table;
    private final MultiTableKeys multiKeys;

    private final List<byte[]> keys = new ArrayList<>();
    private final HashSet<Column> columns = new HashSet<>();

    public TableGetContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        byte[] keysBytes = BufferUtils.getBytes(buffer);
        byte[] columnBytes = BufferUtils.getBytes(buffer);

        ByteBuffer keysBuffer = ByteBuffer.wrap(keysBytes);

        while(!BufferUtils.isOver(keysBuffer)) {
            keys.add(BufferUtils.getBytes(keysBuffer));
        }

        ByteBuffer columnBuffer = ByteBuffer.wrap(columnBytes);

        while(!BufferUtils.isOver(columnBuffer)) {
            byte[] bytes = BufferUtils.getBytes(keysBuffer);
            columns.add(new Column(bytes));
        }

        multiKeys = new MultiTableKeys(keys, columns);
    }

    public String table() {
        return table;
    }

    public MultiTableKeys multiKeys() {
        return multiKeys;
    }

    public List<byte[]> keys() {
        return keys;
    }

    public HashSet<Column> columns() {
        return columns;
    }
}
