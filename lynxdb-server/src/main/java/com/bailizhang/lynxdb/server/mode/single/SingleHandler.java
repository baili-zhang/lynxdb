package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.register.RegisterableEventHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

public class SingleHandler extends RegisterableEventHandler {
    private final SingleLdtpEngine engine;

    public SingleHandler(SingleLdtpEngine singleLdtpEngine) {
        engine = singleLdtpEngine;
    }

    @Override
    protected void handleClientRequest(SocketRequest request) {
        engine.offerInterruptibly(request);
    }

    @Override
    protected void handleEventRegister(SocketRequest request) {

    }
}
