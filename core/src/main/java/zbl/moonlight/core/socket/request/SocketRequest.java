package zbl.moonlight.core.socket.request;

import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketState;

import java.nio.channels.SelectionKey;

public record SocketRequest(boolean isBroadcast, byte status, byte[] data, ServerNode target,
                            SelectionKey selectionKey, Object attachment) {
    public static SocketRequest newBroadcastRequest(byte[] data) {
        byte status = SocketState.STAY_CONNECTED_FLAG | SocketState.BROADCAST_FLAG;
        return new SocketRequest(true, status, data, null, null, null);
    }

    public static SocketRequest newUnicastRequest(byte[] data, ServerNode node) {
        return newUnicastRequest(data, node, null);
    }

    public static SocketRequest newUnicastRequest(byte[] data, ServerNode node, Object attachment) {
        byte status = SocketState.STAY_CONNECTED_FLAG;
        return new SocketRequest(false, status, data, node, null, attachment);
    }

    public static SocketRequest newDisconnectRequest(ServerNode node) {
        byte status = SocketState.EMPTY_FLAG;
        byte[] data = new byte[0];
        return new SocketRequest(false, status, data, node,null, null);
    }
}
