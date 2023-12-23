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

package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

public interface Crc32cUtils {
    static void check(ByteBuffer buffer) {
        buffer.rewind();
        int crc32cOffset = buffer.capacity() - LONG_LENGTH;
        buffer.limit(crc32cOffset);

        CRC32C crc32C = new CRC32C();
        crc32C.update(buffer);
        long crc32cValue = crc32C.getValue();

        buffer.limit(buffer.capacity());
        long storeCrc32cValue = buffer.getLong(crc32cOffset);

        if(crc32cValue != storeCrc32cValue) {
            throw new RuntimeException();
        }
        buffer.rewind();
    }

    static long update(ByteBuffer buffer) {
        // TODO
        return 0L;
    }

    static long update(ByteBuffer buffer, int limit) {
        // TODO
        return 0L;
    }

    static long update(ByteBuffer[] buffers) {
        CRC32C crc32C = new CRC32C();
        for(ByteBuffer buffer : buffers) {
            int position = buffer.position();
            crc32C.update(buffer);
            buffer.position(position);
        }
        return crc32C.getValue();
    }
}
