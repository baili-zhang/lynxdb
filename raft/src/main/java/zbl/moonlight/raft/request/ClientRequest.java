package zbl.moonlight.raft.request;

import zbl.moonlight.core.common.BytesListConvertible;
import zbl.moonlight.socket.common.NioMessage;

import java.nio.channels.SelectionKey;

import static zbl.moonlight.raft.request.RaftRequest.CLIENT_REQUEST;

public class ClientRequest extends NioMessage {
    public ClientRequest(SelectionKey key, BytesListConvertible content) {
        super(key);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.append(content);
    }
}
