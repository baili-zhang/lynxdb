package zbl.moonlight.server.mdtp;

import java.nio.channels.SelectionKey;

public record MdtpCommand (
        SelectionKey selectionKey,
        long serial,
        byte method,
        byte[] content
) {

}
