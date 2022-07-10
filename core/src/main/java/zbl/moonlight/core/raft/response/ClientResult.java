package zbl.moonlight.core.raft.response;

import zbl.moonlight.core.common.BytesConvertible;

public record ClientResult(byte[] data) implements BytesConvertible {
    @Override
    public byte[] toBytes() {
        return data;
    }
}
