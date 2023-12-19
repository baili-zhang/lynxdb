package com.bailizhang.lynxdb.server.engine.params;

import com.bailizhang.lynxdb.core.buffers.Buffers;

public record QueryParams (byte method, Buffers content) {
    public static QueryParams parse(Buffers data) {
        byte method = data.get();
        return new QueryParams(method, data);
    }
}
