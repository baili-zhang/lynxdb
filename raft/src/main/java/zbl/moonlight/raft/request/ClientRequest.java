package zbl.moonlight.raft.request;

import zbl.moonlight.core.common.BytesConvertible;

import java.nio.ByteBuffer;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.raft.request.RaftRequest.CLIENT_REQUEST;

public class ClientRequest implements BytesConvertible {
    private final byte[] command;

    public ClientRequest(byte[] data) {
        command = data;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(BYTE_LENGTH + command.length);
        return buffer.put(CLIENT_REQUEST).put(command).array();
    }
}
