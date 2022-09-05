package com.bailizhang.lynxdb.core.lsm;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils;

import java.nio.ByteBuffer;

public class SSTableEntry {
    public static final byte SET_FLAG = (byte) 0x01;
    public static final byte DELETE_FLAG = (byte) 0x02;

    private final byte status;
    private final String key;
    private final byte[] value;
    private final int totalLength;

    SSTableEntry(byte[] entry) {
        ByteBuffer buffer= ByteBuffer.wrap(entry);
        status = buffer.get();
        key = BufferUtils.getString(buffer);
        value = BufferUtils.getRemaining(buffer);

        /* entry 前面还有一个整型表示 entry 长度 */
        totalLength = entry.length + PrimitiveTypeUtils.INT_LENGTH;
    }

    public boolean isKey(String str) {
        return key.equals(str);
    }

    public boolean isDelete() {
        return status == DELETE_FLAG;
    }

    public String key() {
        return key;
    }
    public byte[] value() {
        return value;
    }

    public int totalLength() {
        return totalLength;
    }
}
