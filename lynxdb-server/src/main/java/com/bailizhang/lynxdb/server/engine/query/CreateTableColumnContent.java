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

public class CreateTableColumnContent implements BytesListConvertible {
    private final String table;
    private final List<byte[]> columns;

    public CreateTableColumnContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        columns = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            columns.add(BufferUtils.getBytes(buffer));
        }
    }

    public CreateTableColumnContent(String table, List<byte[]> columns) {
        this.table = table;
        this.columns = columns;
    }

    public List<byte[]> columns() {
        return columns;
    }

    public String table() {
        return table;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.CREATE_TABLE_COLUMN);
        bytesList.appendVarBytes(G.I.toBytes(table));
        columns.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
