package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.util.zip.CRC32C;

public record DataEntry(
        byte[] data,
        long crc32c
) implements BytesListConvertible {

    public static DataEntry from(byte[] data) {
        CRC32C dataCrc32C = new CRC32C();
        dataCrc32C.update(data);
        long dataCrc32c = dataCrc32C.getValue();

        return new DataEntry(data, dataCrc32c);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawBytes(data);
        bytesList.appendRawLong(crc32c);
        return bytesList;
    }
}
