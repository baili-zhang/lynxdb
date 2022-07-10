package zbl.moonlight.socket.interfaces;

import zbl.moonlight.core.common.BytesConvertible;
import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;

public interface SocketBytesConvertible extends BytesConvertible {
    byte[] toContentBytes();
    void fromBytes(byte[] bytes);

    @Override
    default byte[] toBytes() {
        byte[] content = toContentBytes();
        int length = NumberUtils.INT_LENGTH + content.length;
        return ByteBuffer.allocate(length).putInt(content.length).put(content).array();
    }
}
