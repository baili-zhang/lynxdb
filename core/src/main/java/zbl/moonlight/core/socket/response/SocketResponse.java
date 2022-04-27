package zbl.moonlight.core.socket.response;

import java.nio.channels.SelectionKey;

public record SocketResponse (SelectionKey selectionKey, byte[] data) {
}
