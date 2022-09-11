package com.bailizhang.lynxdb.socket.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import static com.bailizhang.lynxdb.socket.code.Result.REDIRECT;

public record RedirectResult(ServerNode other) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(REDIRECT);
        bytesList.appendVarStr(other.toString());

        return bytesList;
    }
}
