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

public class DropKvStoreContent implements BytesListConvertible {
    private final List<String> kvstores;

    public DropKvStoreContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        kvstores = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            kvstores.add(BufferUtils.getString(buffer));
        }
    }

    public DropKvStoreContent(List<String> kvstores) {
        this.kvstores = kvstores;
    }

    public List<String> kvstores() {
        return kvstores;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.DROP_KV_STORE);
        kvstores.stream().map(G.I::toBytes).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
