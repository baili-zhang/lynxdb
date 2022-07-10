package zbl.moonlight.socket.interfaces;

import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.response.SocketResponse;

public interface SocketClientHandler {
    default void handleConnected(ServerNode node) throws Exception {}
    default void handleAfterLatchAwait() throws Exception {}
    default void handleResponse(SocketResponse response) throws Exception {}
    default void handleConnectFailure(ServerNode node) throws Exception {}
}
