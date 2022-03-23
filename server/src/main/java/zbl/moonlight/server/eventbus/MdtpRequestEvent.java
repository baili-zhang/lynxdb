package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.mdtp.ReadableMdtpRequest;

import java.nio.channels.SelectionKey;

public record MdtpRequestEvent(SelectionKey selectionKey, ReadableMdtpRequest request) {
}
