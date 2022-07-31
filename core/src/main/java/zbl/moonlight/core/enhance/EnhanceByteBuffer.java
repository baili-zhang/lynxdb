package zbl.moonlight.core.enhance;

import java.nio.ByteBuffer;

import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

/**
 * TODO: 改成静态工具类
 */
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

    public byte[] getRemaining() {
        int len = buffer.limit() - buffer.position();
        byte[] remaining = new byte[len];
        buffer.get(remaining);
        return remaining;
    }

    public byte get() {
        return buffer.get();
    }

    public long getLong() {
        return buffer.getLong();
    }

    public Integer getInt() {
        return buffer.getInt();
    }

    /* 判断ByteBuffer是否读结束（或写结束） */
    public static boolean isOver(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            return false;
        }
        return byteBuffer.position() == byteBuffer.limit();
    }

    public static ByteBuffer intByteBuffer() {
        return ByteBuffer.allocate(INT_LENGTH);
    }
}
