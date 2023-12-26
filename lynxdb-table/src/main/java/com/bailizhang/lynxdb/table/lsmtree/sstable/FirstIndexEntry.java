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

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.Crc32cUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public record FirstIndexEntry(
        byte[] beginKey,
        int idx
) implements Comparable<FirstIndexEntry> {
    public static FirstIndexEntry from(ByteBuffer buffer) {
        return new FirstIndexEntry(new byte[]{}, 0);
    }

    public static void writeToBuffer(List<FirstIndexEntry> entries, ByteBuffer buffer) {
        for(FirstIndexEntry entry : entries) {
            int position = buffer.position();
            byte[] beginKey = entry.beginKey;
            BufferUtils.putVarBytes(buffer, beginKey);
            buffer.putInt(entry.idx);
            Crc32cUtils.update(buffer, position, buffer.position());
        }
    }

    @Override
    public int compareTo(FirstIndexEntry o) {
        return Arrays.compare(beginKey, o.beginKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FirstIndexEntry that = (FirstIndexEntry) o;
        return Arrays.equals(beginKey, that.beginKey);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(beginKey);
    }
}
