package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KvValueListRemoveContent implements BytesListConvertible {
    private final String kvstore;
    private final byte[] key;
    private final List<byte[]> values;

    public KvValueListRemoveContent(QueryParams params) {
        values = new ArrayList<>();

        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        kvstore = BufferUtils.getString(buffer);

        key = BufferUtils.getBytes(buffer);
        while(!BufferUtils.isOver(buffer)) {
            values.add(BufferUtils.getBytes(buffer));
        }
    }

    public KvValueListRemoveContent(String kvstore, byte[] key, List<byte[]> values) {
        this.kvstore = kvstore;
        this.key = key;
        this.values = values;
    }

    public String kvstore() {
        return kvstore;
    }

    public byte[] key() {
        return key;
    }

    public List<byte[]> values() {
        return values;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.KV_VALUE_LIST_REMOVE);
        bytesList.appendVarBytes(G.I.toBytes(kvstore));
        bytesList.appendVarBytes(key);

        values.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
