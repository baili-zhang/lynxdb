package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.engine.AffectKey;
import com.bailizhang.lynxdb.server.engine.QueryParams;

import java.nio.ByteBuffer;

public class RegisterKeyContent implements BytesListConvertible {
    private final AffectKey affectKey;

    public RegisterKeyContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        String db = BufferUtils.getString(buffer);
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column  = BufferUtils.getBytes(buffer);

        affectKey = new AffectKey(db, key, columnFamily, column);
    }

    public AffectKey affectKey() {
        return affectKey;
    }

    @Override
    public BytesList toBytesList() {
        return null;
    }
}
