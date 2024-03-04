/*
 * Copyright 2023-2024 Baili Zhang.
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

package com.bailizhang.lynxdb.table.region;

import com.bailizhang.lynxdb.table.config.TableOptions;
import com.bailizhang.lynxdb.table.lsmtree.LsmTree;

import java.nio.file.Path;

public class ColumnRegion extends LsmTree {
    private final String columnFamily;
    private final String column;

    public ColumnRegion(String columnFamily, String column, TableOptions options) {
        super(Path.of(options.baseDir(), columnFamily, column).toString(), options.lsmTreeOptions());
        this.columnFamily = columnFamily;
        this.column = column;
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String column() {
        return column;
    }
}
