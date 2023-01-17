package com.bailizhang.lynxdb.server.engine.timeout;

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;

public record TimeoutValue(
        TimeoutKey timeoutKey,
        byte[] value
) {
    public static TimeoutValue from(ByteBuffer buffer) {
        TimeoutKey timeoutKey = TimeoutKey.from(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        return new TimeoutValue(timeoutKey, value);
    }
}
