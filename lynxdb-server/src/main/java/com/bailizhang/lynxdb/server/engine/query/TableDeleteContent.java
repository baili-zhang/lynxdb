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

public class TableDeleteContent implements BytesListConvertible {
    private final String table;
    private final List<byte[]> keys;

    public TableDeleteContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        keys = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            keys.add(BufferUtils.getBytes(buffer));
        }
    }

    public TableDeleteContent(String table, List<byte[]> keys) {
        this.table = table;
        this.keys = keys;
    }

    public List<byte[]> keys() {
        return keys;
    }

    public String table() {
        return table;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.TABLE_DELETE);
        bytesList.appendVarBytes(G.I.toBytes(table));
        keys.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
