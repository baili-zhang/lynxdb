package zbl.moonlight.core.raft.response;

import zbl.moonlight.core.utils.ByteArrayUtils;

public record RaftResult(int term) implements BytesConvertable {
    @Override
    public byte[] toBytes() {
        return ByteArrayUtils.fromInt(term);
    }
}
