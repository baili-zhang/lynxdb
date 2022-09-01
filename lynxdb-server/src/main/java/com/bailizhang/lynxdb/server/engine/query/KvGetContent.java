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

        bytesList.appendRawByte(LdtpMethod.KV_GET);
        bytesList.appendVarBytes(G.I.toBytes(kvstore));
        keys.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
