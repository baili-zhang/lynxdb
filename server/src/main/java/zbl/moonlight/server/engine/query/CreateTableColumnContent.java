package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateTableColumnContent {
    private final String table;
    private final List<byte[]> columns = new ArrayList<>();

    public CreateTableColumnContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        while(!BufferUtils.isOver(buffer)) {
            columns.add(BufferUtils.getBytes(buffer));
        }
    }

    public List<byte[]> columns() {
        return columns;
    }

    public String table() {
        return table;
    }
}
