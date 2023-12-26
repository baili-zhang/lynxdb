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

import com.bailizhang.lynxdb.core.utils.Crc32cUtils;

import java.nio.ByteBuffer;
import java.util.List;

public record SecondIndexEntry(
        byte flag, // 是否删除
        int begin, // 顺序查找不需要，二分查找需要这个字段
        int length
) {
    public static SecondIndexEntry from(ByteBuffer buffer) {
        int position = buffer.position();
        byte flag = buffer.get();
        int begin = buffer.getInt();
        int length = buffer.getInt();

        Crc32cUtils.check(buffer, position, buffer.position());

        return new SecondIndexEntry(flag, begin, length);
    }

    public static void writeToBuffer(List<SecondIndexEntry> entries, ByteBuffer buffer) {
        for(SecondIndexEntry entry : entries) {
            int position = buffer.position();
            buffer.put(entry.flag);
            buffer.putInt(entry.begin);
            buffer.putInt(entry.length);
            Crc32cUtils.update(buffer, position, buffer.position());
        }
    }
}
