package zbl.moonlight.core.utils;

import java.nio.ByteBuffer;

public class ByteArrayUtils {
    public static int toInt(byte[] data) {
        if(data.length != 4) {
            throw new IllegalStateException("Byte array length must be 4.");
        }
        return ByteBuffer.wrap(data).getInt();
    }
}
