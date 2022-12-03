package com.bailizhang.lynxdb.client;

import java.nio.ByteBuffer;

/**
 * 解析不同结构的返回数据
 */
public class LynxDbResult {
    private final ByteBuffer buffer;
    private final byte status;
    private String message;

    public LynxDbResult(byte[] value) {
        buffer = ByteBuffer.wrap(value);
        status = buffer.get();
    }
}
