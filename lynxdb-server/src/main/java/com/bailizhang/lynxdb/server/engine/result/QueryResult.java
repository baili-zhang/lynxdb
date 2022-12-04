package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;

import java.util.List;

public record QueryResult (BytesList data, List<AffectValue> affectValues) {
}
