package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;

public interface ByteArrayUtils {
    byte[] EMPTY_BYTES = new byte[0];

    static int compare(byte[] origin, byte[] target) {
        int minLen = Math.min(origin.length, target.length);

        for(int i = 0; i < minLen; i ++) {
            if(origin[i] > target[i]) {
                return 1;
            } else if(origin[i] < target[i]) {
                return -1;
            }
        }

        return origin.length - target.length;
    }

    static boolean isEmpty(byte[] src) {
        return src == null || src.length == 0;
    }

    static int toInt(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getInt();
    }

    static long toLong(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getLong();
    }
}
