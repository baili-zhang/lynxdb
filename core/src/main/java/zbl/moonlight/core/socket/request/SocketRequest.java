package zbl.moonlight.core.socket.request;

import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketState;

import java.nio.channels.SelectionKey;

public record SocketRequest(boolean isBroadcast, byte status, byte[] data, ServerNode serverNode,
                            SelectionKey selectionKey) {
    public static SocketRequest newBroadcastRequest(byte[] data) {
        byte status = SocketState.STAY_CONNECTED_FLAG | SocketState.BROADCAST_FLAG;
        return new SocketRequest(true, status, data, null, null);
    }

    public static SocketRequest newUnicastRequest(byte[] data, ServerNode node) {
        assert node != null;
        byte status = SocketState.STAY_CONNECTED_FLAG;
        return new SocketRequest(false, status, data, node, null);
    }
}
