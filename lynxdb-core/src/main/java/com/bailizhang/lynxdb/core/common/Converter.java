package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.charset.Charset;

public record Converter(Charset charset) {
    public byte[] toBytes(String src) {
        if(src == null) {
            return BufferUtils.EMPTY_BYTES;
        }

        return src.getBytes(charset);
    }
}
