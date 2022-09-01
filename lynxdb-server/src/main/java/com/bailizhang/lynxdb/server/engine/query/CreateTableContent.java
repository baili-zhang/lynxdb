package com.bailizhang.lynxdb.server.engine.query;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateTableContent implements BytesListConvertible {
    private final List<String> tables;

    public CreateTableContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        tables = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            tables.add(BufferUtils.getString(buffer));
        }
    }

    public CreateTableContent(List<String> tables) {
        this.tables = tables;
    }

    public List<String> tables() {
        return tables;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(LdtpMethod.CREATE_TABLE);
        tables.stream().map(G.I::toBytes).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
