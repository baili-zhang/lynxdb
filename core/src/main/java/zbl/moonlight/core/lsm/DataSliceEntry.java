package zbl.moonlight.core.lsm;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;

import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

public class DataSliceEntry {
    public static final byte SET_FLAG = (byte) 0x01;
    public static final byte DELETE_FLAG = (byte) 0x02;

    private final byte status;
    private final String key;
    private final byte[] value;
    private final int totalLength;

    DataSliceEntry(byte[] entry) {
        EnhanceByteBuffer buffer= EnhanceByteBuffer.wrap(entry);
        status = buffer.get();
        key = buffer.getString();
        value = buffer.getRemaining();

        /* entry 前面还有一个整型表示 entry 长度 */
        totalLength = entry.length + INT_LENGTH;
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
