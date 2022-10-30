package com.bailizhang.lynxdb.core.utils;

public interface NameUtils {
    int DEFAULT_NAME_LENGTH = 8;
    String ZERO = "0";

    static String name(int id) {
        String idStr = String.valueOf(id);
        int zeroCount = DEFAULT_NAME_LENGTH - idStr.length();
        return ZERO.repeat(Math.max(0, zeroCount)) + idStr + FileUtils.LOG_SUFFIX;
    }
}
