package com.bailizhang.lynxdb.raft.response;

import com.bailizhang.lynxdb.core.common.BytesConvertible;

public record ClientResult(byte[] data) implements BytesConvertible {
    @Override
    public byte[] toBytes() {
        return data;
    }
}
