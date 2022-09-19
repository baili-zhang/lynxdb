package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.Key;
import com.bailizhang.lynxdb.storage.core.MultiTableRows;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TableInsertContent implements BytesListConvertible {
    private final String table;
    private final MultiTableRows rows;

    public TableInsertContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        List<Key> keys = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        int keySize = buffer.getInt();

        for(int i = 0; i < keySize; i ++) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            keys.add(new Key(bytes));
        }

        int columnSize = buffer.getInt();

        for(int i = 0; i < columnSize; i ++) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            columns.add(new Column(bytes));
        }

        while(!BufferUtils.isOver(buffer)) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            values.add(bytes);
        }

        rows = new MultiTableRows();

        int i = 0;
        for(Key key : keys) {
            Map<Column, byte[]> row = new HashMap<>();

            for(Column column : columns) {
                row.put(column, values.get(i ++));
            }

            rows.put(key, row);
        }
    }

    public TableInsertContent(String table, MultiTableRows rows) {
        this.table = table;
        this.rows = rows;
    }

    public String table() {
        return table;
    }

    public MultiTableRows rows() {
        return rows;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.TABLE_INSERT);
        bytesList.appendVarBytes(G.I.toBytes(table));

        List<Key> keys = rows.keySet().stream().toList();

        if(keys.size() == 0) {
            throw new RuntimeException("rows is empty");
        }

        List<Column> columns = rows.get(keys.get(0)).keySet().stream().toList();

        int keySize = keys.size();
        int columnSize = columns.size();

        bytesList.appendRawInt(keySize);
        keys.stream().map(Key::value).forEach(bytesList::appendVarBytes);

        bytesList.appendRawInt(columnSize);
        columns.stream().map(Column::value).forEach(bytesList::appendVarBytes);

        for(Key key : keys) {
            Map<Column, byte[]> row = rows.get(key);
            for (Column column : columns) {
                byte[] value = row.get(column);
                bytesList.appendVarBytes(value);
            }
        }

        return bytesList;
    }
}
