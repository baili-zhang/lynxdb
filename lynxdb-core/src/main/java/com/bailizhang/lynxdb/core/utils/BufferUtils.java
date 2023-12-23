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

package com.bailizhang.lynxdb.core.utils;

import com.bailizhang.lynxdb.core.common.G;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public interface BufferUtils {
    static String getString(ByteBuffer buffer) {
        return G.I.toString(BufferUtils.getBytes(buffer));
    }

    static byte[] getBytes(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }

    static byte[] getRemaining(ByteBuffer buffer) {
        int len = buffer.limit() - buffer.position();
        byte[] remaining = new byte[len];
        buffer.get(remaining);
        return remaining;
    }

    static String getRemainingString(ByteBuffer buffer) {
        byte[] bytes = getRemaining(buffer);
        return new String(bytes);
    }

    static byte[] toBytes(Object o) {
        switch (o) {
            case String str -> {
                return G.I.toBytes(str);
            }

            case Byte b -> {
                return new byte[]{b};
            }

            case Short sht -> {
                ByteBuffer buffer = ByteBuffer.allocate(SHORT_LENGTH);
                return buffer.putShort(sht).array();
            }

            case Integer i -> {
                return intByteBuffer(i).array();
            }

            case Long l -> {
                ByteBuffer buffer = ByteBuffer.allocate(LONG_LENGTH);
                return buffer.putLong(l).array();
            }

            case Character c -> {
                return G.I.toBytes(String.valueOf(c));
            }

            case Float f -> {
                ByteBuffer buffer = ByteBuffer.allocate(FLOAT_LENGTH);
                return buffer.putFloat(f).array();
            }

            case Double d -> {
                ByteBuffer buffer = ByteBuffer.allocate(DOUBLE_LENGTH);
                return buffer.putDouble(d).array();
            }

            default -> throw new IllegalStateException("Unsupported parameter type: " + o.getClass().getName());
        }
    }

    /* 判断ByteBuffer是否读结束（或写结束） */
    static boolean isOver(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            throw new RuntimeException();
        }
        return byteBuffer.position() == byteBuffer.limit();
    }

    static boolean isOver(ByteBuffer[] buffers) {
        int len = buffers.length;
        if(len == 0) {
            throw new RuntimeException();
        }

        ByteBuffer lastBuffer = buffers[len-1];
        return lastBuffer.position() == lastBuffer.limit();
    }

    static boolean isNotOver(ByteBuffer byteBuffer) {
        return !isOver(byteBuffer);
    }

    static ByteBuffer byteByteBuffer(byte value) {
        return ByteBuffer.allocate(BYTE_LENGTH).put(value).rewind();
    }

    static ByteBuffer intByteBuffer() {
        return ByteBuffer.allocate(INT_LENGTH);
    }

    static ByteBuffer intByteBuffer(int value) {
        return ByteBuffer.allocate(INT_LENGTH).putInt(value).rewind();
    }

    static ByteBuffer longByteBuffer(long value) {
        return ByteBuffer.allocate(LONG_LENGTH).putLong(value).rewind();
    }

    static void write(ByteBuffer buffer, int offset, ByteBuffer[] data) {
        for(ByteBuffer dataBuffer : data) {
            if(BufferUtils.isOver(dataBuffer)) {
                continue;
            }

            int dataPosition = dataBuffer.position();

            int rem = buffer.limit() - offset;
            int dataRem = dataBuffer.limit() - dataPosition;
            int writeLen = Math.min(rem, dataRem);

            buffer.put(offset, dataBuffer, dataPosition, writeLen);

            offset += writeLen;
            buffer.position(offset);

            dataPosition += writeLen;
            dataBuffer.position(dataPosition);

            if(BufferUtils.isOver(buffer)) {
                return;
            }
        }
    }

    static ByteBuffer[] toBuffers(byte[] ...data) {
        int len = data.length;
        ByteBuffer[] buffers = new ByteBuffer[len];
        for(int i = 0; i < len; i ++) {
            buffers[i] = ByteBuffer.wrap(data[i]);
        }
        return buffers;
    }

    static int length(ByteBuffer[] buffers) {
        int len = 0;
        for(ByteBuffer buffer : buffers) {
            len += buffer.limit();
        }
        return len;
    }
}
