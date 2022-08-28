package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.common.BytesListConvertible;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KvGetContent implements BytesListConvertible {
    private final String kvstore;
    private final List<byte[]> keys;

    public KvGetContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        kvstore = BufferUtils.getString(buffer);
        keys = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            keys.add(BufferUtils.getBytes(buffer));
        }
    }

    public KvGetContent(String kvstore, List<byte[]> keys) {
        this.kvstore = kvstore;
        this.keys = keys;
    }

    public List<byte[]> keys() {
        return keys;
    }

    public String kvstore() {
        return kvstore;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendVarBytes(G.I.toBytes(kvstore));
        keys.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
