package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.MdtpMethod;
import com.bailizhang.lynxdb.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DropTableContent implements BytesListConvertible {
    private final List<String> tables;

    public DropTableContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        tables = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            tables.add(BufferUtils.getString(buffer));
        }
    }

    public DropTableContent(List<String> tables) {
        this.tables = tables;
    }

    public List<String> tables() {
        return tables;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.DROP_TABLE);
        tables.stream().map(G.I::toBytes).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
