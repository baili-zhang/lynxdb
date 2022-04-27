package zbl.moonlight.core.socket.request;

import zbl.moonlight.core.socket.client.ServerNode;

import java.nio.channels.SelectionKey;

public record SocketRequest(boolean isBroadcast, byte status, byte[] data, ServerNode serverNode,
                            SelectionKey selectionKey) {
    public static SocketRequest newBroadcastRequest(byte status, byte[] data) {
        return new SocketRequest(true, status, data, null, null);
    }

    public static SocketRequest newUnicastRequest(byte status, byte[] data, ServerNode node) {
        assert node != null;
        return new SocketRequest(false, status, data, node, null);
    }
}
