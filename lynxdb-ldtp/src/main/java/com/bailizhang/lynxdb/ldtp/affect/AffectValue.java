package com.bailizhang.lynxdb.ldtp.affect;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.ldtp.message.MessageType;

import java.nio.ByteBuffer;
import java.util.HashMap;

public record AffectValue(
        MessageKey messageKey,
        HashMap<String, byte[]> multiColumns
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        BytesList key = messageKey.toBytesList();

        bytesList.appendRawByte(MessageType.AFFECT);
        bytesList.append(key);
        multiColumns.forEach((column, value) -> {
            bytesList.appendRawBytes(G.I.toBytes(column));
            bytesList.appendVarBytes(value);
        });

        return bytesList;
    }

    public static AffectValue from(ByteBuffer buffer) {
        MessageKey messageKey = MessageKey.from(buffer);
        HashMap<String, byte[]> dbValues = valuesFrom(buffer);

        return new AffectValue(messageKey, dbValues);
    }

    public static HashMap<String, byte[]> valuesFrom(ByteBuffer buffer) {
        HashMap<String, byte[]> multiColumns = new HashMap<>();

        while (BufferUtils.isNotOver(buffer)) {
            String column = BufferUtils.getString(buffer);
            byte[] value = BufferUtils.getBytes(buffer);
            multiColumns.put(column, value);
        }

        return multiColumns;
    }
}
