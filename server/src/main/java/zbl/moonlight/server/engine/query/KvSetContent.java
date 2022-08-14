package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KvSetContent {
    private final String kvstore;
    private List<Pair<byte[], byte[]>> kvPairs = new ArrayList<>();

    public KvSetContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        kvstore = BufferUtils.getString(buffer);
    }

    public List<Pair<byte[], byte[]>> kvPairs () {
        return kvPairs;
    }

    public String kvstore() {
        return kvstore;
    }
}
