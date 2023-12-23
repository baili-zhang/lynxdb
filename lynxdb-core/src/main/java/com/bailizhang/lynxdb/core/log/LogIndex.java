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

package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public record LogIndex(
        byte deleteFlag,
        int dataBegin,
        int dataLength,
        long crc32c
) {
    public static final int ENTRY_LENGTH = BYTE_LENGTH + INT_LENGTH * 2 + LONG_LENGTH;

    public static LogIndex from(ByteBuffer buffer) {
        byte flag = buffer.get();
        int dataBegin = buffer.getInt();
        int dataLength = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(dataBegin);
        crc32C.update(dataLength);

        long crc32CValue = crc32C.getValue();

        if(crc32CValue != crc32c) {
            throw new RuntimeException("Log Index data error");
        }

        return new LogIndex(
                flag,
                dataBegin,
                dataLength,
                crc32c
        );
    }

    public static LogIndex from(
            byte deleteFlag,
            int dataBegin,
            int dataLength
    ) {
        CRC32C indexCrc32C = new CRC32C();
        indexCrc32C.update(new byte[]{deleteFlag});
        indexCrc32C.update(dataBegin);
        indexCrc32C.update(dataLength);
        long indexCrc32c = indexCrc32C.getValue();

        return new LogIndex(
                deleteFlag,
                dataBegin,
                dataLength,
                indexCrc32c
        );
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(deleteFlag);
        dataBlocks.appendRawInt(dataBegin);
        dataBlocks.appendRawInt(dataLength);
        dataBlocks.appendRawLong(crc32c);
        return dataBlocks.toBuffers();
    }
}
