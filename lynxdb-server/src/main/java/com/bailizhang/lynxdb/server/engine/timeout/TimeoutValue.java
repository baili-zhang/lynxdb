package com.bailizhang.lynxdb.server.engine.timeout;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.engine.message.MessageType;

import java.nio.ByteBuffer;

public record TimeoutValue(
        TimeoutKey timeoutKey,
        byte[] value
) implements BytesListConvertible {
    public static TimeoutValue from(ByteBuffer buffer) {
        TimeoutKey timeoutKey = TimeoutKey.from(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        return new TimeoutValue(timeoutKey, value);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        BytesList key = timeoutKey.toBytesList();

        bytesList.appendRawByte(MessageType.TIMEOUT);
        bytesList.append(key);
        bytesList.appendVarBytes(value);

        return bytesList;
    }
}
