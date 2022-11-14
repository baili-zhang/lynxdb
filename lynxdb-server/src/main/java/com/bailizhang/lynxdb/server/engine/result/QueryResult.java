package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.server.engine.AffectKey;

import java.util.List;

public record QueryResult (BytesList data, List<AffectKey> affectKeys) {
}
