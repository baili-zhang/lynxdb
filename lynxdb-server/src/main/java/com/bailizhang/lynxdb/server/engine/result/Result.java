package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.G;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.core.utils.NumberUtils.BYTE_LENGTH;
import static com.bailizhang.lynxdb.server.engine.result.Result.Error.INVALID_ARGUMENT;

public interface Result {
    byte SUCCESS = (byte) 0x01;
    byte SUCCESS_WITH_LIST = (byte) 0x02;
    byte SUCCESS_WITH_KV_PAIRS = (byte) 0x03;
    byte SUCCESS_WITH_TABLE = (byte) 0x04;

    interface Error {
        byte INVALID_ARGUMENT = (byte) 0x70;
    }

    static byte[] invalidArgument(String message) {
        byte[] msgBytes = G.I.toBytes(message);

        ByteBuffer buffer = ByteBuffer.allocate(msgBytes.length + BYTE_LENGTH);
        return buffer.put(INVALID_ARGUMENT).put(msgBytes).array();
    }
}
