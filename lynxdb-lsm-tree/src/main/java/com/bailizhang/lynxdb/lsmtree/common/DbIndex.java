package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.FileChannelUtils;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public record DbIndex(
        DbKey key,
        int valueGlobalIndex
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.append(key);
        bytesList.appendRawInt(valueGlobalIndex);
        return bytesList;
    }

    public static List<DbIndex> listFrom(FileChannel channel, int dataBegin) {
        List<DbIndex> dbIndexList = new ArrayList<>();

        long length = FileChannelUtils.size(channel);

        while(dataBegin < length - 1) {
            int keyLength = FileChannelUtils.readInt(channel, dataBegin);
            dataBegin += INT_LENGTH;
            byte[] key = FileChannelUtils.read(channel, dataBegin, keyLength);
            dataBegin += keyLength;

            int columnLength = FileChannelUtils.readInt(channel, dataBegin);
            dataBegin += INT_LENGTH;
            byte[] column = FileChannelUtils.read(channel, dataBegin, columnLength);
            dataBegin += columnLength;

            int valueGlobalIndex = FileChannelUtils.readInt(channel, dataBegin);
            dataBegin += INT_LENGTH;

            DbKey dbKey = new DbKey(key, column);
            DbIndex dbIndex = new DbIndex(dbKey, valueGlobalIndex);

            dbIndexList.add(dbIndex);
        }

        return dbIndexList;
    }
}
