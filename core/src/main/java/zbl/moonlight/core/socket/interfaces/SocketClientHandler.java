package zbl.moonlight.core.socket.interfaces;

import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.response.SocketResponse;

public interface SocketClientHandler {
    default void handleConnected(ServerNode node) {}
    default void handleAfterLatchAwait() {}
    default void handleResponse(SocketResponse response) {}
}
