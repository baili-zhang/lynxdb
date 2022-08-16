package zbl.moonlight.raft.request;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;

public class ClientRequest extends RaftRequest {
    private final byte[] command;

    public ClientRequest(SelectionKey selectionKey, byte[] data) {
        super(selectionKey);
        command = data;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(BYTE_LENGTH + command.length);
        return buffer.put(CLIENT_REQUEST).put(command).array();
    }
}
