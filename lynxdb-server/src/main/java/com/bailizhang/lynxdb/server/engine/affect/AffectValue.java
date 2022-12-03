package com.bailizhang.lynxdb.server.engine.affect;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

public record AffectValue (
        AffectKey affectKey,
        byte[] value
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        return null;
    }
}
