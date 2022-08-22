package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TableDeleteContent {
    private final String table;
    private final List<byte[]> keys = new ArrayList<>();

    public TableDeleteContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        while(!BufferUtils.isOver(buffer)) {
            keys.add(BufferUtils.getBytes(buffer));
        }
    }

    public List<byte[]> keys() {
        return keys;
    }

    public String table() {
        return table;
    }
}
