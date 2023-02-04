package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.DB_VALUE_LIST;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod.*;


public class LdtpStorageEngine extends BaseStorageEngine {
    public LdtpStorageEngine() {
        super(LdtpStorageEngine.class);
    }

    @LdtpMethod(FIND_BY_KEY_CF_COLUMN)
    public QueryResult doFindByKeyCfColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);

        byte[] value = dataLsmTree.find(key, columnFamily, column);

        BytesList bytesList = new BytesList();

        if(value == null) {
            bytesList.appendRawByte(LdtpCode.NULL);
        } else {
            bytesList.appendRawByte(LdtpCode.BYTE_ARRAY);
            bytesList.appendRawBytes(value);
        }

        return new QueryResult(bytesList, null);
    }

    @LdtpMethod(FIND_BY_KEY_CF)
    public QueryResult doFindByKeyCf(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);

        return doFindByKeyCfColumn(key, columnFamily);
    }

    public QueryResult doFindByKeyCfColumn(byte[] key, byte[] columnFamily) {
        List<DbValue> values = dataLsmTree.find(key, columnFamily);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(DB_VALUE_LIST);
        values.forEach(bytesList::append);

        return new QueryResult(bytesList, null);
    }

    @LdtpMethod(INSERT)
    public QueryResult doInsert(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);

        dataLsmTree.insert(key, columnFamily, column, value);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(INSERT_MULTI_COLUMN)
    public QueryResult doInsertMultiColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);

        List<DbValue> dbValues = new ArrayList<>();

        while(BufferUtils.isNotOver(buffer)) {
            byte[] column = BufferUtils.getBytes(buffer);
            byte[] value = BufferUtils.getBytes(buffer);

            dbValues.add(new DbValue(column, value));
        }

        dataLsmTree.insert(key, columnFamily, dbValues);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(DELETE)
    public QueryResult doDelete(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);

        dataLsmTree.delete(key, columnFamily, column);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(DELETE_MULTI_COLUMN)
    public QueryResult doDeleteMultiColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);

        dataLsmTree.delete(key, columnFamily);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }
}
