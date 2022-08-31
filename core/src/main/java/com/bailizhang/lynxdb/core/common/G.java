package com.bailizhang.lynxdb.core.common;

public class G {
    public static final G I = new G();

    private Converter converter;

    private G() {

    }

    public void converter(Converter cvt) {
        if(converter == null) {
            converter = cvt;
        }
    }

    public byte[] toBytes(String src) {
        return converter.toBytes(src);
    }
}
