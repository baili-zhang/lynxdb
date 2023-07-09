package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.Pair;

import java.util.HashMap;
import java.util.List;

/**
 * 支持：列族，列
 */
public interface Table {
    byte[] find(byte[] key, String columnFamily, String column);
    HashMap<String, byte[]> findMultiColumns(byte[] key, String columnFamily, String... findColumn);

    List<Pair<byte[], HashMap<String, byte[]>>> rangeNext(
            String columnFamily,
            String mainColumn,
            byte[] beginKey,
            int limit,
            String... findColumns
    );

    List<Pair<byte[], HashMap<String, byte[]>>> rangeBefore(
            String columnFamily,
            String mainColumn,
            byte[] endKey,
            int limit,
            String... findColumns
    );

    void insert(
            byte[] key,
            String columnFamily,
            String column,
            byte[] value,
            long timeout
    );
    void insert(
            byte[] key,
            String columnFamily,
            HashMap<String, byte[]> multiColumns,
            long timeout
    );
    boolean insertIfNotExisted(
            byte[] key,
            String columnFamily,
            HashMap<String,byte[]> multiColumns,
            long timeout
    );

    void delete(byte[] key, String columnFamily, String column);
    void deleteMultiColumns(byte[] key, String columnFamily, String... deleteColumns);

    boolean existKey(
            byte[] key,
            String columnFamily,
            String mainColumn
    );

    void clear();
}
