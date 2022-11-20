package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.server.engine.AffectValue;

import java.util.ArrayList;
import java.util.List;

import static com.bailizhang.lynxdb.server.annotations.LdtpCode.INVALID_ARGUMENT;
import static com.bailizhang.lynxdb.server.annotations.LdtpCode.SUCCESS;

public interface Result {
    static QueryResult invalidArgument(String message) {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(INVALID_ARGUMENT);
        bytesList.appendRawStr(message);

        return new QueryResult(bytesList, new ArrayList<>());
    }

    static QueryResult success() {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(SUCCESS);

        return new QueryResult(bytesList, new ArrayList<>());
    }

    static QueryResult success(List<AffectValue> affectValues) {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(SUCCESS);

        return new QueryResult(bytesList, new ArrayList<>());
    }
}
