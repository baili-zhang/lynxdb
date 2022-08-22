package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DropKvStoreContent {
    private final List<String> kvstores = new ArrayList<>();

    public DropKvStoreContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        while(!BufferUtils.isOver(buffer)) {
            kvstores.add(BufferUtils.getString(buffer));
        }
    }

    public List<String> kvstores() {
        return kvstores;
    }
}
