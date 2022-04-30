package zbl.moonlight.core.raft.response;

public record ClientResult(byte[] data) implements BytesConvertable {
    @Override
    public byte[] toBytes() {
        return data;
    }
}
