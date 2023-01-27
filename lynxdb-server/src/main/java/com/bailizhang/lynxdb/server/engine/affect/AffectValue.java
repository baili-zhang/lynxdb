package com.bailizhang.lynxdb.server.engine.affect;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;
import com.bailizhang.lynxdb.server.engine.message.MessageType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public record AffectValue(
        MessageKey messageKey,
        List<DbValue> dbValues
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        BytesList key = messageKey.toBytesList();

        bytesList.appendRawByte(MessageType.AFFECT);
        bytesList.append(key);
        dbValues.forEach(bytesList::append);

        return bytesList;
    }

    public static AffectValue from(ByteBuffer buffer) {
        MessageKey messageKey = MessageKey.from(buffer);
        List<DbValue> dbValues = new ArrayList<>();

        while (BufferUtils.isNotOver(buffer)) {
            dbValues.add(DbValue.from(buffer));
        }

        return new AffectValue(messageKey, dbValues);
    }
}
