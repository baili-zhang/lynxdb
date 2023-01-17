package com.bailizhang.lynxdb.server.engine.timeout;

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;

public record TimeoutKey(
        byte[] key,
        byte[] columnFamily
) {
    public static TimeoutKey from(ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        return new TimeoutKey(key, columnFamily);
    }
}
