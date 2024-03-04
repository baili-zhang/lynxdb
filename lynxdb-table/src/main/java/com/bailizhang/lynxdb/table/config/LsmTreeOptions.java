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

package com.bailizhang.lynxdb.table.config;

public class LsmTreeOptions {
    private static final int DEFAULT_MEM_TABLE_SIZE = 2000;
    private final int memTableSize;

    private boolean wal = true;

    public LsmTreeOptions() {
        this(DEFAULT_MEM_TABLE_SIZE);
    }

    public LsmTreeOptions(int memTableSize) {
        this.memTableSize = memTableSize;
    }

    public int memTableSize() {
        return memTableSize;
    }

    public boolean wal() {
        return wal;
    }

    public void wal(boolean val) {
        wal = val;
    }
}
