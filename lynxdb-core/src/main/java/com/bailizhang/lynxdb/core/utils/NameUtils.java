package com.bailizhang.lynxdb.core.utils;

public interface NameUtils {
    int DEFAULT_NAME_LENGTH = 8;
    String ZERO = "0";
    String EMPTY_STR = "";

    static String name(int id) {
        String idStr = String.valueOf(id);
        int zeroCount = DEFAULT_NAME_LENGTH - idStr.length();
        return ZERO.repeat(Math.max(0, zeroCount)) + idStr + FileUtils.LOG_SUFFIX;
    }

    static int id(String name) {
        String id = name.replace(FileUtils.LOG_SUFFIX, EMPTY_STR);
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }
}
