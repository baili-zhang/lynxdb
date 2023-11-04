package com.bailizhang.lynxdb.core.utils;

public interface NameUtils {
    int DEFAULT_NAME_LENGTH = 8;
    String ZERO = "0";

    static String name(int id) {
        String idStr = String.valueOf(id);
        int zeroCount = DEFAULT_NAME_LENGTH - idStr.length();
        return ZERO.repeat(Math.max(0, zeroCount)) + idStr;
    }

    static int id(String name) {
        try {
            if(name.charAt(DEFAULT_NAME_LENGTH) != '.') {
                throw new RuntimeException();
            }

            return Integer.parseInt(name.substring(0, DEFAULT_NAME_LENGTH));
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }
}
