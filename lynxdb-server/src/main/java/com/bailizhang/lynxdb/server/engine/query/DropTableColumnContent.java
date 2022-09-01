package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.storage.core.Column;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class DropTableColumnContent implements BytesListConvertible {
    private final String table;
    private final HashSet<Column> columns;

    public DropTableColumnContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        columns = new HashSet<>();

        while(!BufferUtils.isOver(buffer)) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            columns.add(new Column(bytes));
        }
    }

    public DropTableColumnContent(String table, HashSet<Column> columns) {
        this.table = table;
        this.columns = columns;
    }

    public HashSet<Column> columns() {
        return columns;
    }

    public String table() {
        return table;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.DROP_TABLE_COLUMN);
        bytesList.appendVarBytes(G.I.toBytes(table));
        columns.stream().map(Column::value).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
