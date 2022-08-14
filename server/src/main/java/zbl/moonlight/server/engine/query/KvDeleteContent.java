package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KvDeleteContent {
    private final String kvstore;
    private final List<byte[]> keys = new ArrayList<>();

    public KvDeleteContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        kvstore = BufferUtils.getString(buffer);

        while(!BufferUtils.isOver(buffer)) {
            keys.add(BufferUtils.getBytes(buffer));
        }
    }

    public List<byte[]> keys() {
        return keys;
    }

    public String kvstore() {
        return kvstore;
    }
}
