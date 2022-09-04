package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.MultiTableKeys;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;


public class TableSelectContent implements BytesListConvertible {
    private final String table;
    private final MultiTableKeys multiKeys;

    private final List<byte[]> keys;
    private final HashSet<Column> columns;

    public TableSelectContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        keys = new ArrayList<>();
        columns = new HashSet<>();

        int keySize = buffer.getInt();
        for(int i = 0; i < keySize; i ++) {
            keys.add(BufferUtils.getBytes(buffer));
        }

        int columnSize = buffer.getInt();
        for(int i = 0; i < columnSize; i ++) {
            columns.add(new Column(BufferUtils.getBytes(buffer)));
        }

        multiKeys = new MultiTableKeys(keys, columns);
    }

    public TableSelectContent(String table, MultiTableKeys multiKeys) {
        this.table = table;
        this.keys = multiKeys.left();
        this.columns = multiKeys.right();
        this.multiKeys = multiKeys;
    }

    public String table() {
        return table;
    }

    public MultiTableKeys multiKeys() {
        return multiKeys;
    }

    public List<byte[]> keys() {
        return keys;
    }

    public HashSet<Column> columns() {
        return columns;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.TABLE_SELECT);
        bytesList.appendVarBytes(G.I.toBytes(table));

        int keySize = keys.size();
        int columnSize = columns.size();

        bytesList.appendRawInt(keySize);
        keys.forEach(bytesList::appendVarBytes);
        bytesList.appendRawInt(columnSize);
        columns.stream().map(Column::value).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
