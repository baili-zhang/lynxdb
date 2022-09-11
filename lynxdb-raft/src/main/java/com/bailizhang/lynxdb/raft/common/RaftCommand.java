package com.bailizhang.lynxdb.raft.common;

import java.nio.channels.SelectionKey;

public record RaftCommand (SelectionKey selectionKey, boolean isDataChanged, byte[] command) {
}
