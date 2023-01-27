package com.bailizhang.lynxdb.server.engine.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;

public record QueryResult (BytesList data, MessageKey messageKey) {
}
