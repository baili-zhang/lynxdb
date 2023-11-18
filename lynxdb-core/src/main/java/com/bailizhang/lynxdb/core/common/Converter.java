package com.bailizhang.lynxdb.core.common;

import java.nio.charset.Charset;

public record Converter(Charset charset) {
    public byte[] toBytes(String src) {
        if(src == null) {
            return Bytes.EMPTY;
        }

        return src.getBytes(charset);
    }

    public String toString(byte[] src) {
        return new String(src, charset);
    }
}
