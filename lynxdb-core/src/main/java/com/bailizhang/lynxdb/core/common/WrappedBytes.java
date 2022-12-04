package com.bailizhang.lynxdb.core.common;

import java.util.Arrays;

public class WrappedBytes implements Comparable<WrappedBytes>{
    private final byte[] bytes;

    public WrappedBytes(byte[] val) {
        bytes = val;
    }

    public byte[] bytes() {
        return bytes;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WrappedBytes that = (WrappedBytes) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public int compareTo(WrappedBytes o) {
        return Arrays.compare(bytes, o.bytes);
    }
}
