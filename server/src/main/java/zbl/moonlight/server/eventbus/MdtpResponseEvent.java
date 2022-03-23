package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.mdtp.WritableMdtpResponse;

import java.nio.channels.SelectionKey;

public record MdtpResponseEvent (SelectionKey selectionKey, WritableMdtpResponse response) {
}
