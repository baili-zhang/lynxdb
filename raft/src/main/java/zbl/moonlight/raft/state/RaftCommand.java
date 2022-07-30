package zbl.moonlight.raft.state;

import java.nio.channels.SelectionKey;

public record RaftCommand (SelectionKey selectionKey, byte[] command) {
}
