package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

        logger.debug("Find by key: {}, columnFamily: {}, column: {}, value is: {}.",
                G.I.toString(key), columnFamily, column, G.I.toString(value));

        DataBlocks dataBlocks = new DataBlocks();

        if(value == null) {
            dataBlocks.appendRawByte(LdtpCode.NULL);
        } else {
            dataBlocks.appendRawByte(LdtpCode.BYTE_ARRAY);
            dataBlocks.appendRawBytes(value);
        }

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(FIND_MULTI_COLUMNS)
    public QueryResult doFindMultiColumns(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String[] findColumns = null;

        if(BufferUtils.isNotOver(buffer)) {
            List<String> columns = new ArrayList<>();
            while(BufferUtils.isNotOver(buffer)) {
                String findColumn = BufferUtils.getString(buffer);
                columns.add(findColumn);
            }
            findColumns = columns.toArray(String[]::new);
        }

        return doFindMultiColumns(key, columnFamily, findColumns);
    }

    public QueryResult doFindMultiColumns(byte[] key, String columnFamily, String... findColumns) {
        HashMap<String, byte[]> multiColumns = dataTable.findMultiColumns(key, columnFamily, findColumns);

        logger.debug("Find by key: {}, columnFamily: {}.", G.I.toString(key), columnFamily);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(MULTI_COLUMNS);
        appendMultiColumns(dataBlocks, multiColumns);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(INSERT)
    public QueryResult doInsert(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String column = BufferUtils.getString(buffer);
        long timeout = buffer.getLong();
        byte[] value = BufferUtils.getBytes(buffer);

        logger.debug("Insert key: {}, columnFamily: {}, column: {}, timeout: {}, value: {}.",
                G.I.toString(key), columnFamily, column, timeout, G.I.toString(value));

        dataTable.insert(key, columnFamily, column, value, timeout);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(VOID);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(INSERT_MULTI_COLUMNS)
    public QueryResult doInsertMultiColumns(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        long timeout = buffer.getLong();

        HashMap<String, byte[]> multiColumns = new HashMap<>();

        while(BufferUtils.isNotOver(buffer)) {
            String column = BufferUtils.getString(buffer);
            byte[] value = BufferUtils.getBytes(buffer);

            multiColumns.put(column, value);
        }

        logger.debug("Insert key: {}, columnFamily: {}, multiColumns: {}, timeout: {}.",
                G.I.toString(key), columnFamily, multiColumns, timeout);

        dataTable.insert(key, columnFamily, multiColumns, timeout);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(VOID);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(INSERT_IF_NOT_EXISTED)
    public QueryResult doInsertIfNotExisted(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        long timeout = buffer.getLong();

        HashMap<String, byte[]> multiColumns = new HashMap<>();

        while(BufferUtils.isNotOver(buffer)) {
            String column = BufferUtils.getString(buffer);
            byte[] value = BufferUtils.getBytes(buffer);

            multiColumns.put(column, value);
        }

        logger.debug("Insert if not existed, key: {}, columnFamily: {}, multiColumns: {}, timeout: {}.",
                G.I.toString(key), columnFamily, multiColumns, timeout);

        boolean success = dataTable.insertIfNotExisted(key, columnFamily, multiColumns, timeout);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(success ? TRUE : FALSE);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(DELETE)
    public QueryResult doDelete(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String column = BufferUtils.getString(buffer);

        logger.debug("Delete key: {}, columnFamily: {}, column: {}.",
                G.I.toString(key), columnFamily, column);

        dataTable.delete(key, columnFamily, column);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(VOID);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(DELETE_MULTI_COLUMNS)
    public QueryResult doDeleteMultiColumns(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String[] deleteColumns = null;

        if(!BufferUtils.isNotOver(buffer)) {
            List<String> columns = new ArrayList<>();
            while(!BufferUtils.isNotOver(buffer)) {
                String column = BufferUtils.getString(buffer);
                columns.add(column);
            }
            deleteColumns = columns.toArray(String[]::new);
        }

        logger.debug("Delete dbKey: {}, columnFamily: {}, deleteColumns: {}.",
                G.I.toString(key), columnFamily, deleteColumns);

        dataTable.deleteMultiColumns(key, columnFamily, deleteColumns);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(VOID);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(RANGE_NEXT)
    public QueryResult doRangeNext(QueryParams params) {
        return range(params, dataTable::rangeNext);
    }

    @LdtpMethod(RANGE_BEFORE)
    public QueryResult doRangeBefore(QueryParams params) {
        return range(params, dataTable::rangeBefore);
    }

    @LdtpMethod(EXIST_KEY)
    public QueryResult doExistKey(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);
        String mainColumn = BufferUtils.getString(buffer);

        boolean existed = dataTable.existKey(key, columnFamily, mainColumn);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(existed ? TRUE : FALSE);

        return new QueryResult(dataBlocks);
    }

    /**
     * Append multiColumns to bytesList
     *
     * @param dataBlocks bytesList
     * @param multiColumns multiColumns
     */
    private void appendMultiColumns(
            DataBlocks dataBlocks,
            HashMap<String, byte[]> multiColumns
    ) {
        multiColumns.forEach((column, value) -> {
            dataBlocks.appendVarStr(column);

            if(value == null) {
                dataBlocks.appendRawByte(NULL);
            } else {
                dataBlocks.appendRawByte(BYTE_ARRAY);
                dataBlocks.appendVarBytes(value);
            }
        });
    }

    private QueryResult range(QueryParams params, RangeOperator operator) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        String columnFamily = BufferUtils.getString(buffer);
        String mainColumn = BufferUtils.getString(buffer);
        byte[] baseKey = BufferUtils.getBytes(buffer);
        int limit = buffer.getInt();
        String[] findColumns = null;

        if(BufferUtils.isNotOver(buffer)) {
            List<String> columns = new ArrayList<>();
            while(BufferUtils.isNotOver(buffer)) {
                String column = BufferUtils.getString(buffer);
                columns.add(column);
            }
            findColumns = columns.toArray(String[]::new);
        }

        logger.info("Do range search, columnFamily: {}, mainColumn: {}, baseKey: {}, limit: {}, findColumns: {}.",
                columnFamily, mainColumn, G.I.toString(baseKey), limit, findColumns);

        var multiKeys = operator.doRange(
                columnFamily,
                mainColumn,
                baseKey,
                limit,
                findColumns
        );

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(MULTI_KEYS);

        for(var pair : multiKeys) {
            byte[] key = pair.left();
            var multiColumns = pair.right();
            int size = multiColumns.size();

            dataBlocks.appendVarBytes(key);
            dataBlocks.appendRawInt(size);
            appendMultiColumns(dataBlocks, multiColumns);
        }

        return new QueryResult(dataBlocks);
    }

    @FunctionalInterface
    private interface RangeOperator {
        List<Pair<byte[], HashMap<String, byte[]>>> doRange(
                String columnFamily,
                String mainColumn,
                byte[] baseKey,
                int limit,
                String... findColumns
        );
    }
}
