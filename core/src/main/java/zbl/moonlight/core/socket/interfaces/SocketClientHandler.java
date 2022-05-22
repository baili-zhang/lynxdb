package zbl.moonlight.core.socket.interfaces;

import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.response.SocketResponse;

public interface SocketClientHandler {
    default void handleConnected(ServerNode node) throws Exception {}
    default void handleAfterLatchAwait() throws Exception {}
    default void handleResponse(SocketResponse response) throws Exception {}
    default void handleConnectFailure(ServerNode node) throws Exception {}
}
