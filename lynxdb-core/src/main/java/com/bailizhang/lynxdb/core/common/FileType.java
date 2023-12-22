/*
 * Copyright 2023 Baili Zhang.
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

package com.bailizhang.lynxdb.core.common;

public enum FileType {
    LOG_GROUP_MANAGE_FILE(1001, ".lgm"),
    LOG_GROUP_REGION_FILE(1002, ".lgr"),
    SSTABLE_FILE(2001, ".sst");

    private final int magicNumber;
    private final String suffix;

    FileType(int magicNumber, String suffix) {
        this.magicNumber = magicNumber;
        this.suffix = suffix;
    }

    public int magicNumber() {
        return magicNumber;
    }

    public String suffix() {
        return suffix;
    }
}
