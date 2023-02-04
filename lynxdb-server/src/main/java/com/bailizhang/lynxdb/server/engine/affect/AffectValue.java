package com.bailizhang.lynxdb.server.engine.affect;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.ldtp.message.MessageType;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;

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
        List<DbValue> dbValues = valuesFrom(buffer);

        return new AffectValue(messageKey, dbValues);
    }

    public static List<DbValue> valuesFrom(ByteBuffer buffer) {
        List<DbValue> dbValues = new ArrayList<>();

        while (BufferUtils.isNotOver(buffer)) {
            dbValues.add(DbValue.from(buffer));
        }

        return dbValues;
    }
}
