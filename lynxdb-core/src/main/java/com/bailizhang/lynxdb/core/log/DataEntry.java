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

package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.utils.Crc32cUtils;

import java.nio.ByteBuffer;

public record DataEntry(
        ByteBuffer[] data,
        long crc32c
) {

    public static DataEntry from(ByteBuffer[] data) {
        long crc32c = Crc32cUtils.update(data);
        return new DataEntry(data, crc32c);
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawBuffers(data);
        dataBlocks.appendRawLong(crc32c);
        return dataBlocks.toBuffers();
    }
}
