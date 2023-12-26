/*
 * Copyright 2022-2023 Baili Zhang.
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

package com.bailizhang.lynxdb.table;

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
