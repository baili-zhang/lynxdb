package com.bailizhang.lynxdb.core.common;

import java.util.Arrays;

/**
 * 给 lsm tree 用的 key
 */
public record Key(byte[] value, boolean flag) implements Comparable<Key> {
    public static final boolean SET = true;
    public static final boolean DELETE = false;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Arrays.equals(value, key.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public int compareTo(Key o) {
        byte[] val = o.value;
        int len = Math.min(value.length, val.length);

        for (int i = 0; i < len; i++) {
            if(value[i] > val[i]) {
                return 1;
            } else if(value[i] < val[i]) {
                return -1;
            }
        }

        return value.length - val.length;
    }
}
