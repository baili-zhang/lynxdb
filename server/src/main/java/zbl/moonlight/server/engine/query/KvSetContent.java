package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.common.BytesListConvertible;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KvSetContent implements BytesListConvertible {
    private final String kvstore;
    private final List<Pair<byte[], byte[]>> kvPairs;

    public KvSetContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        kvstore = BufferUtils.getString(buffer);

        kvPairs = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            byte[] key = BufferUtils.getBytes(buffer);
            byte[] value = BufferUtils.getBytes(buffer);

            kvPairs.add(new Pair<>(key, value));
        }
    }

    public KvSetContent(String kvstore, List<Pair<byte[], byte[]>> kvPairs) {
        this.kvstore = kvstore;
        this.kvPairs = kvPairs;
    }

    public List<Pair<byte[], byte[]>> kvPairs() {
        return kvPairs;
    }

    public String kvstore() {
        return kvstore;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.KV_SET);
        bytesList.appendVarBytes(G.I.toBytes(kvstore));
        kvPairs.forEach(pair -> {
            bytesList.appendVarBytes(pair.left());
            bytesList.appendVarBytes(pair.right());
        });

        return bytesList;
    }
}
