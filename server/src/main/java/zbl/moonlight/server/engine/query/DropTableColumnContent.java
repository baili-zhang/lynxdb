package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Column;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class DropTableColumnContent {
    private final String table;
    private final HashSet<Column> columns = new HashSet<>();

    public DropTableColumnContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        while(!BufferUtils.isOver(buffer)) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            columns.add(new Column(bytes));
        }
    }

    public HashSet<Column> columns() {
        return columns;
    }

    public String table() {
        return table;
    }
}
