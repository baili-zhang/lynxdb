package zbl.moonlight.server.engine.result;

import zbl.moonlight.core.common.G;

import java.nio.ByteBuffer;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.server.engine.result.Result.Error.INVALID_ARGUMENT;

public interface Result {
    byte SUCCESS = (byte) 0x01;

    interface Error {
        byte INVALID_ARGUMENT = (byte) 0x02;
    }

    static byte[] invalidArgument(String message) {
        byte[] msgBytes = G.I.toBytes(message);

        ByteBuffer buffer = ByteBuffer.allocate(msgBytes.length + BYTE_LENGTH);
        return buffer.put(INVALID_ARGUMENT).put(msgBytes).array();
    }
}
