package zbl.moonlight.core.protocol.common;

import java.nio.channels.SelectionKey;

public record WritableEvent(SelectionKey selectionKey, Writable writable) {
}
