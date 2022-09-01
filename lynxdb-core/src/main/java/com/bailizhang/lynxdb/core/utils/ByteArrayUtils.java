package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;

public interface ByteArrayUtils {
    public static int toInt(byte[] data) {
        if(data.length != 4) {
            throw new IllegalStateException("Byte array length must be 4.");
        }
        return ByteBuffer.wrap(data).getInt();
    }

    public static byte[] fromInt(int i) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(i);
        return buffer.array();
    }
}
