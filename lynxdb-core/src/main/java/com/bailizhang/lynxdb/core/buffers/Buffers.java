package com.bailizhang.lynxdb.core.buffers;

import com.bailizhang.lynxdb.core.utils.ArrayUtils;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public record Buffers(
        ByteBuffer[] buffers
) {
    public int length() {
        int len = 0;
        for(ByteBuffer buffer : buffers) {
            len += buffer.limit();
        }
        return len;
    }

    public byte get() {
        for(ByteBuffer buffer : buffers) {
            if(BufferUtils.isNotOver(buffer)) {
                return buffer.get();
            }
        }
        throw new BufferOverflowException();
    }

    public int getInt() {
        // TODO
        return 0;
    }

    public long getLong() {
        // TODO
        return 0L;
    }

    public Buffers nextPart() {
        // TODO
        return new Buffers(null);
    }

    public String nextStringPart() {
        // TODO
        return "";
    }

    public boolean hasRemaining() {
        return ArrayUtils.last(buffers).hasRemaining();
    }

    public byte[] toBytes() {
        // TODO 记录拷贝的总时间
        int len = length();
        ByteBuffer buffer = ByteBuffer.allocate(len);
        Arrays.stream(buffers).forEach(buffer::put);
        return buffer.array();
    }
}
