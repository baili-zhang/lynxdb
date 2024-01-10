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

package com.bailizhang.lynxdb.core.buffers;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ArrayUtils;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * TODO: 优化拷贝
 */
public class Buffers {
    private final ByteBuffer[] buffers;
    private final int length;

    private int idx = 0;

    public Buffers(ByteBuffer[] buffers) {
        this.buffers = buffers;

        int length = 0;
        for(ByteBuffer buffer : buffers) {
            length += buffer.limit();
        }

        this.length = length;
    }

    public void rewind() {
        for(ByteBuffer buffer : buffers) {
            buffer.rewind();
        }
    }

    public int length() {
        return length;
    }

    public byte[] getVar(int len) {
        byte[] data = new byte[len];

        int i = 0;
        while (i < len) {
            while (BufferUtils.isOver(buffers[idx])) {
                idx ++;
            }

            while (BufferUtils.isNotOver(buffers[idx]) && i < len) {
                data[i++] = buffers[idx].get();
            }
        }

        return data;
    }

    public byte get() {
        return getVar(1)[0];
    }

    public int getInt() {
        return ArrayUtils.toInt(getVar(INT_LENGTH));
    }

    public long getLong() {
        return ArrayUtils.toInt(getVar(LONG_LENGTH));
    }

    public byte[] nextPartBytes() {
        int len = getInt();
        return getVar(len);
    }

    public String nextStringPart() {
        int len = getInt();
        return G.I.toString(getVar(len));
    }

    public boolean hasRemaining() {
        return ArrayUtils.last(buffers).hasRemaining();
    }

    public byte[] toBytes() {
        int len = length();
        ByteBuffer buffer = ByteBuffer.allocate(len);
        Arrays.stream(buffers).forEach(buffer::put);
        return buffer.array();
    }
}
