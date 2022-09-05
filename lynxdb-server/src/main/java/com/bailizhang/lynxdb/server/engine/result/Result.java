package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.BytesList;

import static com.bailizhang.lynxdb.server.engine.result.Result.Error.INVALID_ARGUMENT;

public interface Result {
    byte SUCCESS = (byte) 0x01;
    byte SUCCESS_WITH_LIST = (byte) 0x02;
    byte SUCCESS_WITH_KV_PAIRS = (byte) 0x03;
    byte SUCCESS_WITH_TABLE = (byte) 0x04;

    interface Error {
        byte INVALID_ARGUMENT = (byte) 0x70;
    }

    static BytesList invalidArgument(String message) {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(INVALID_ARGUMENT);
        bytesList.appendRawStr(message);

        return bytesList;
    }

    static BytesList success() {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(SUCCESS);
        return bytesList;
    }
}
