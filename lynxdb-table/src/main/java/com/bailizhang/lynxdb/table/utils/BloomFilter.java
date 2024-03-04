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

package com.bailizhang.lynxdb.table.utils;

import com.bailizhang.lynxdb.core.mmap.MappedBuffer;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.BYTE_BIT_COUNT;

/**
 * 28 个 hash 函数，bit 位应该是插入元素的 40 倍
 * 误判率约等于 3.37e-9
 */
public class BloomFilter {
    public static final int BITS_TIMES = 40;
    private static final int HASH_FUNC_SIZE = 28;

    private final MappedBuffer mappedBuffer;

    private BloomFilter(MappedBuffer buffer) {
        mappedBuffer = buffer;
    }

    public static BloomFilter from(Path filePath, int begin, int count) {
        int length = (count * BITS_TIMES) / BYTE_BIT_COUNT;
        MappedBuffer buffer = new MappedBuffer(filePath, begin, length);

        return new BloomFilter(buffer);
    }

    public boolean isExist(byte[] key) {
        MappedByteBuffer buffer = mappedBuffer.getBuffer();
        int bitCount = mappedBuffer.length() * BYTE_BIT_COUNT;

        for(int i = 1; i < HASH_FUNC_SIZE + 1; i ++) {
            int hash = hashCode(key, i);
            int remainder = hash % bitCount;

            // 第几个 byte
            int byteIndex = remainder / BYTE_BIT_COUNT;
            // byte 中的第几个 bit 位
            int bitIndex = remainder % BYTE_BIT_COUNT;

            byte current = buffer.get(byteIndex);
            if((current & ((byte) 0x01 << bitIndex)) == 0) {
                return false;
            }
        }

        return true;
    }

    public boolean isNotExist(byte[] key) {
        return !isExist(key);
    }

    public void setObj(byte[] key) {
        MappedByteBuffer buffer = mappedBuffer.getBuffer();
        int bitCount = mappedBuffer.length() * BYTE_BIT_COUNT;

        for(int i = 1; i < HASH_FUNC_SIZE + 1; i ++) {
            int hash = hashCode(key, i);
            int remainder = hash % bitCount;

            // 第几个 byte
            int byteIndex = remainder / BYTE_BIT_COUNT;
            // byte 中的第几个 bit 位
            int bitIndex = remainder % BYTE_BIT_COUNT;

            byte current = buffer.get(byteIndex);
            current |= (byte) ((byte) 0x01 << bitIndex);
            buffer.put(byteIndex, current);
        }
    }

    public int length() {
        return mappedBuffer.length();
    }

    public void force() {
        mappedBuffer.force();
    }

    private static int hashCode(byte[] key, int n) {
        int hash = 0;
        for (byte b : key) {
            hash += b;
            hash += (hash << n + 3);
            hash ^= (hash >> n);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return (hash & 0x7FFFFFFF);
    }
}
