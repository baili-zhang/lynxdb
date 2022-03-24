package zbl.moonlight.server.eventbus;

import zbl.moonlight.core.protocol.mdtp.WritableMdtpResponse;

import java.nio.channels.SelectionKey;

public record MdtpResponseEvent (SelectionKey selectionKey, WritableMdtpResponse response) {
}
