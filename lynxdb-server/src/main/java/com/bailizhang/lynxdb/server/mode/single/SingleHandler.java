package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

public class SingleHandler implements SocketServerHandler {
    private final SingleLdtpEngine engine;

    public SingleHandler(SingleLdtpEngine singleLdtpEngine) {
        engine = singleLdtpEngine;
    }

    @Override
    public void handleRequest(SocketRequest request) {
        engine.offerInterruptibly(request);
    }
}
