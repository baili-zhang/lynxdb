package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.*;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod.*;


public class LdtpStorageEngine extends BaseStorageEngine {
    private static final Logger logger = LoggerFactory.getLogger(LdtpStorageEngine.class);

    public LdtpStorageEngine() {
        super(LdtpStorageEngine.class);
    }

    @LdtpMethod(FIND_BY_KEY_CF_COLUMN)
    public QueryResult doFindByKeyCfColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String column = BufferUtils.getString(buffer);

        byte[] value = dataTable.find(key, columnFamily, column);

        logger.debug("Find by dbKey: {}, columnFamily: {}, column: {}, value is: {}.",
                G.I.toString(key), columnFamily, column, G.I.toString(value));

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
        String columnFamily = BufferUtils.getString(buffer);

        return doFindByKeyCf(key, columnFamily);
    }

    public QueryResult doFindByKeyCf(byte[] key, String columnFamily) {
        HashMap<String, byte[]> multiColumns = dataTable.find(key, columnFamily);

        logger.debug("Find by dbKey: {}, columnFamily: {}.", G.I.toString(key), columnFamily);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(MULTI_COLUMNS);
        appendMultiColumns(bytesList, multiColumns);

        return new QueryResult(bytesList, null);
    }

    @LdtpMethod(INSERT)
    public QueryResult doInsert(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String column = BufferUtils.getString(buffer);
        byte[] value = BufferUtils.getBytes(buffer);

        logger.debug("Insert dbKey: {}, columnFamily: {}, column: {}, value: {}.",
                G.I.toString(key), columnFamily, column, G.I.toString(value));

        dataTable.insert(key, columnFamily, column, value);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(INSERT_MULTI_COLUMN)
    public QueryResult doInsertMultiColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);

        HashMap<String, byte[]> multiColumns = new HashMap<>();

        while(BufferUtils.isNotOver(buffer)) {
            String column = BufferUtils.getString(buffer);
            byte[] value = BufferUtils.getBytes(buffer);

            multiColumns.put(column, value);
        }

        logger.debug("Insert dbKey: {}, columnFamily: {}, multiColumns: {}.",
                G.I.toString(key), columnFamily, multiColumns);

        dataTable.insert(key, columnFamily, multiColumns);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(DELETE)
    public QueryResult doDelete(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String column = BufferUtils.getString(buffer);

        logger.debug("Delete dbKey: {}, columnFamily: {}, column: {}.",
                G.I.toString(key), columnFamily, column);

        dataTable.delete(key, columnFamily, column);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(DELETE_MULTI_COLUMN)
    public QueryResult doDeleteMultiColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);

        logger.debug("Delete dbKey: {}, columnFamily: {}.",
                G.I.toString(key), columnFamily);

        dataTable.delete(key, columnFamily);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(RANGE_NEXT)
    public QueryResult doRangeNext(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        String columnFamily = BufferUtils.getString(buffer);
        String mainColumn = BufferUtils.getString(buffer);
        byte[] beginKey = BufferUtils.getBytes(buffer);
        int limit = buffer.getInt();

        var multiKeys = dataTable.rangeNext(columnFamily, mainColumn, beginKey, limit);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(MULTI_KEYS);

        for(var entry : multiKeys.entrySet()) {
            byte[] key = entry.getKey();
            var multiColumns = entry.getValue();
            int size = multiColumns.size();

            bytesList.appendVarBytes(key);
            bytesList.appendRawInt(size);
            appendMultiColumns(bytesList, multiColumns);
        }

        return new QueryResult(bytesList, null);
    }

    @LdtpMethod(RANGE_BEFORE)
    public QueryResult doRangeBefore(QueryParams params) {
        throw new UnsupportedOperationException();
    }

    @LdtpMethod(EXIST_KEY)
    public QueryResult doExistKey(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String mainColumn = BufferUtils.getString(buffer);

        boolean existed = dataTable.existKey(key, columnFamily, mainColumn);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(existed ? TRUE : FALSE);

        return new QueryResult(bytesList, null);
    }

    private void appendMultiColumns(
            BytesList bytesList,
            HashMap<String, byte[]> multiColumns
    ) {
        multiColumns.forEach((column, value) -> {
            bytesList.appendVarStr(column);

            if(value == null) {
                bytesList.appendRawByte(NULL);
            } else {
                bytesList.appendRawByte(BYTE_ARRAY);
                bytesList.appendVarBytes(value);
            }
        });
    }
}
