/*
 * Copyright 2022-2024 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.buffers.Buffers;
import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.common.Tuple;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Buffers content = params.content();

        byte[] key = content.nextPartBytes();
        String columnFamily = content.nextStringPart();
        String column = content.nextStringPart();

        byte[] value = dataTable.find(key, columnFamily, column);

        logger.debug("Find by key: {}, columnFamily: {}, column: {}, value is: {}.",
                G.I.toString(key), columnFamily, column, G.I.toString(value));

        DataBlocks dataBlocks = new DataBlocks(false);

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
        Buffers content = params.content();

        byte[] key = content.nextPartBytes();
        String columnFamily = content.nextStringPart();
        String[] findColumns = null;

        if(content.hasRemaining()) {
            List<String> columns = new ArrayList<>();
            while(content.hasRemaining()) {
                String findColumn = content.nextStringPart();
                columns.add(findColumn);
            }
            findColumns = columns.toArray(String[]::new);
        }

        return doFindMultiColumns(key, columnFamily, findColumns);
    }

    public QueryResult doFindMultiColumns(byte[] key, String columnFamily, String... findColumns) {
        HashMap<String, byte[]> multiColumns = dataTable.findMultiColumns(key, columnFamily, findColumns);

        logger.debug("Find by key: {}, columnFamily: {}.", G.I.toString(key), columnFamily);

        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(MULTI_COLUMNS);
        appendMultiColumns(dataBlocks, multiColumns);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(INSERT)
    public QueryResult doInsert(QueryParams params) {
        Buffers content = params.content();

        String columnFamily = content.nextStringPart();
        String column = content.nextStringPart();

        List<Tuple<byte[], byte[], Long>> kvPairs = new ArrayList<>();

        StringBuilder pairsMsg = new StringBuilder();
        while (content.hasRemaining()) {
            byte[] key = content.nextPartBytes();
            byte[] value = content.nextPartBytes();
            long timeout = content.getLong();

            kvPairs.add(new Tuple<>(key, value, timeout));
            pairsMsg.append(String.format(
                    "{ key: %s, value: %s, timeout: %d }",
                    G.I.toString(key),
                    G.I.toString(value),
                    timeout
            ));
        }

        logger.debug("Insert into columnFamily: {}, column: {}, kvPairs: {}", columnFamily, column, pairsMsg);

        dataTable.insert(columnFamily, column, kvPairs);

        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(VOID);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(INSERT_MULTI_COLUMNS)
    public QueryResult doInsertMultiColumns(QueryParams params) {
        Buffers content = params.content();

        byte[] key = content.nextPartBytes();
        String columnFamily = content.nextStringPart();
        long timeout = content.getLong();

        HashMap<String, byte[]> multiColumns = new HashMap<>();

        while(content.hasRemaining()) {
            String column = content.nextStringPart();
            byte[] value = content.nextPartBytes();

            multiColumns.put(column, value);
        }

        logger.debug("Insert key: {}, columnFamily: {}, multiColumns: {}, timeout: {}.",
                G.I.toString(key), columnFamily, multiColumns, timeout);

        dataTable.insert(key, columnFamily, multiColumns, timeout);

        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(VOID);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(INSERT_IF_NOT_EXISTED)
    public QueryResult doInsertIfNotExisted(QueryParams params) {
        Buffers content = params.content();

        byte[] key = content.nextPartBytes();
        String columnFamily = content.nextStringPart();
        long timeout = content.getLong();

        HashMap<String, byte[]> multiColumns = new HashMap<>();

        while(content.hasRemaining()) {
            String column = content.nextStringPart();
            byte[] value = content.nextPartBytes();

            multiColumns.put(column, value);
        }

        logger.debug("Insert if not existed, key: {}, columnFamily: {}, multiColumns: {}, timeout: {}.",
                G.I.toString(key), columnFamily, multiColumns, timeout);

        boolean success = dataTable.insertIfNotExisted(key, columnFamily, multiColumns, timeout);

        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(success ? TRUE : FALSE);

        return new QueryResult(dataBlocks);
    }

    @LdtpMethod(DELETE)
    public QueryResult doDeleteMultiColumns(QueryParams params) {
        Buffers content = params.content();

        byte[] key = content.nextPartBytes();
        String columnFamily = content.nextStringPart();
        String[] deleteColumns = null;

        if(content.hasRemaining()) {
            List<String> columns = new ArrayList<>();
            while(content.hasRemaining()) {
                String column = content.nextStringPart();
                columns.add(column);
            }
            deleteColumns = columns.toArray(String[]::new);
        }

        logger.debug("Delete dbKey: {}, columnFamily: {}, deleteColumns: {}.",
                G.I.toString(key), columnFamily, deleteColumns);

        dataTable.deleteMultiColumns(key, columnFamily, deleteColumns);

        DataBlocks dataBlocks = new DataBlocks(false);
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
        Buffers content = params.content();

        byte[] key = content.nextPartBytes();
        String columnFamily = content.nextStringPart();
        String mainColumn = content.nextStringPart();

        boolean existed = dataTable.existKey(key, columnFamily, mainColumn);

        DataBlocks dataBlocks = new DataBlocks(false);
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
        Buffers content = params.content();

        String columnFamily = content.nextStringPart();
        String mainColumn = content.nextStringPart();
        byte[] baseKey = content.nextPartBytes();
        int limit = content.getInt();
        String[] findColumns = null;

        if(content.hasRemaining()) {
            List<String> columns = new ArrayList<>();
            while(content.hasRemaining()) {
                String column = content.nextStringPart();
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

        DataBlocks dataBlocks = new DataBlocks(false);
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
