package zbl.moonlight.core.utils;

import java.nio.ByteBuffer;

public class ByteBufferUtils {
    /* 判断ByteBuffer是否读结束（或写结束） */
    public static boolean isOver(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            return false;
        }
        return byteBuffer.position() == byteBuffer.limit();
    }

    public static ByteBuffer intByteBuffer() {
        return ByteBuffer.allocate(4);
    }
}
