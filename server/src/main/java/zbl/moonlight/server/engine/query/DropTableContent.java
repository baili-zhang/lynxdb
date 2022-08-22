package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DropTableContent {
    private final List<String> tables = new ArrayList<>();

    public DropTableContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        while(!BufferUtils.isOver(buffer)) {
            tables.add(BufferUtils.getString(buffer));
        }
    }

    public List<String> tables() {
        return tables;
    }
}
