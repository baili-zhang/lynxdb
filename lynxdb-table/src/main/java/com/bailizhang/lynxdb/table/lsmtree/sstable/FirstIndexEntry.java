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

package com.bailizhang.lynxdb.table.lsmtree.sstable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public record FirstIndexEntry(
        byte[] beginKey,
        int idx
) implements Comparable<FirstIndexEntry> {
    public static FirstIndexEntry from(ByteBuffer buffer) {
        return new FirstIndexEntry(null, 0);
    }

    public static void writeToBuffer(List<FirstIndexEntry> entries, ByteBuffer buffer) {

    }

    @Override
    public int compareTo(FirstIndexEntry o) {
        return Arrays.compare(beginKey, o.beginKey);
    }
}
