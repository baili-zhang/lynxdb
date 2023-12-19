package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SegmentSocketRequest;

public class SingleHandler implements SocketServerHandler {
    private final LdtpEngineExecutor engineExecutor;

    public SingleHandler(LdtpEngineExecutor executor) {
        engineExecutor = executor;
    }

    @Override
    public void handleRequest(SegmentSocketRequest request) {
        engineExecutor.offerInterruptibly(request);
    }
}
