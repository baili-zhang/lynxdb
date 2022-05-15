package zbl.moonlight.core.enhance;

import java.nio.ByteBuffer;

public class EnhanceByteBuffer {
    private final ByteBuffer buffer;

    private EnhanceByteBuffer(byte[] bytes) {
        buffer = ByteBuffer.wrap(bytes);
    }

    private EnhanceByteBuffer(int len) {
        buffer = ByteBuffer.allocate(len);
    }

    public static EnhanceByteBuffer wrap(byte[] bytes) {
        return new EnhanceByteBuffer(bytes);
    }

    public static EnhanceByteBuffer allocate(int len) {
        return new EnhanceByteBuffer(len);
    }

    public String getString() {
        return new String(getBytes());
    }

    public byte[] getBytes() {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }

    public byte get() {
        return buffer.get();
    }
}
