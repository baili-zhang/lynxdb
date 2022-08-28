package zbl.moonlight.socket.interfaces;

import zbl.moonlight.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;

public interface SocketClientHandler {
    default void handleConnected(SelectionKey selectionKey) throws Exception {}
    default void handleAfterLatchAwait() throws Exception {}
    default void handleResponse(SocketResponse response) throws Exception {}
    default void handleConnectFailure(SelectionKey selectionKey) throws Exception {}
    default void handleDisconnect(SelectionKey selectionKey) throws Exception {}
}
