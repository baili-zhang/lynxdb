package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.nio.charset.Charset;

public record Converter(Charset charset) {
    public byte[] toBytes(String src) {
        if(src == null) {
            return ByteArrayUtils.EMPTY_BYTES;
        }

        return src.getBytes(charset);
    }

    public String toString(byte[] src) {
        return new String(src, charset);
    }
}
