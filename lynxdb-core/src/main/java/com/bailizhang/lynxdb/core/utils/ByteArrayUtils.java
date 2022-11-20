package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;

public interface ByteArrayUtils {
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

    static int toInt(byte[] extraData) {
        ByteBuffer buffer = ByteBuffer.wrap(extraData);
        return buffer.getInt();
    }
}
