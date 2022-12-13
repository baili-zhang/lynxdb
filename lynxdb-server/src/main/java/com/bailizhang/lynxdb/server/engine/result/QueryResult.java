package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;

public record QueryResult (BytesList data, AffectKey affectKey) {
}
