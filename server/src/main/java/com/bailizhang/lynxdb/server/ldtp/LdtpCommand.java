package com.bailizhang.lynxdb.server.ldtp;

import java.nio.channels.SelectionKey;

public record LdtpCommand(
        SelectionKey selectionKey,
        long serial,
        byte method,
        byte[] content
) {

}
