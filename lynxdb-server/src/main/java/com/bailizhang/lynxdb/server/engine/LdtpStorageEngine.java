package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
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

        BytesList bytesList = new BytesList();

        if(value == null) {
            bytesList.appendRawByte(LdtpCode.NULL);
        } else {
            bytesList.appendRawByte(LdtpCode.BYTE_ARRAY);
            bytesList.appendRawBytes(value);
        }

        return new QueryResult(bytesList, null);
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

        logger.debug("Insert key: {}, columnFamily: {}, column: {}, value: {}.",
                G.I.toString(key), columnFamily, column, G.I.toString(value));

        dataTable.insert(key, columnFamily, column, value);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(INSERT_MULTI_COLUMNS)
    public QueryResult doInsertMultiColumns(QueryParams params) {
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

        logger.debug("Insert key: {}, columnFamily: {}, multiColumns: {}.",
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

        logger.debug("Delete key: {}, columnFamily: {}, column: {}.",
                G.I.toString(key), columnFamily, column);

        dataTable.delete(key, columnFamily, column);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
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

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new MessageKey(key, columnFamily));
    }

    @LdtpMethod(RANGE_NEXT)
    public QueryResult doRangeNext(QueryParams params) {
        logger.info("Handle range next.");

        return range(params, dataTable::rangeNext);
    }

    @LdtpMethod(RANGE_BEFORE)
    public QueryResult doRangeBefore(QueryParams params) {
        logger.info("Handle range before.");

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

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(existed ? TRUE : FALSE);

        return new QueryResult(bytesList, null);
    }

    /**
     * Append multiColumns to bytesList
     *
     * @param bytesList bytesList
     * @param multiColumns multiColumns
     */
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

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(MULTI_KEYS);

        for(var pair : multiKeys) {
            byte[] key = pair.left();
            var multiColumns = pair.right();
            int size = multiColumns.size();

            bytesList.appendVarBytes(key);
            bytesList.appendRawInt(size);
            appendMultiColumns(bytesList, multiColumns);
        }

        return new QueryResult(bytesList, null);
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
