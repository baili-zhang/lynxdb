package zbl.moonlight.core.protocol.common;

import java.nio.channels.SelectionKey;

public record ReadableEvent(SelectionKey selectionKey, Readable readable) {
}
