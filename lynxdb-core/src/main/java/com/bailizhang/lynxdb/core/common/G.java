package com.bailizhang.lynxdb.core.common;

import java.util.HashMap;

public class G {
    public static final G I = new G();

    private final HashMap<String, Long> records = new HashMap<>();

    private Converter converter;

    private G() {

    }

    public void converter(Converter cvt) {
        if(converter == null) {
            converter = cvt;
        }
    }

    public void incrementRecord(String name, long increment) {
        long value = records.getOrDefault(name, 0L);
        records.put(name, value + increment);
    }

    public void printRecord() {
        System.out.println(records);
    }

    public byte[] toBytes(String src) {
        return converter.toBytes(src);
    }

    public String toString(byte[] src) {
        return converter.toString(src);
    }
}
