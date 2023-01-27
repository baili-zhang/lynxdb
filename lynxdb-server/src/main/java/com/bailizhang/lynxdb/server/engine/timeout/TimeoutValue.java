package com.bailizhang.lynxdb.server.engine.timeout;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;
import com.bailizhang.lynxdb.server.engine.message.MessageType;

import java.nio.ByteBuffer;

public record TimeoutValue(
        MessageKey messageKey,
        byte[] value
) implements BytesListConvertible {
    public static TimeoutValue from(ByteBuffer buffer) {
        MessageKey messageKey = MessageKey.from(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        return new TimeoutValue(messageKey, value);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        BytesList key = messageKey.toBytesList();

        bytesList.appendRawByte(MessageType.TIMEOUT);
        bytesList.append(key);
        bytesList.appendVarBytes(value);

        return bytesList;
    }
}
