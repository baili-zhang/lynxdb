package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.mode.AffectKeyRegistry;
import com.bailizhang.lynxdb.socket.code.Request;
import com.bailizhang.lynxdb.socket.register.RegisterableEventHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;

import java.nio.channels.SelectionKey;
import java.util.List;

public class SingleHandler extends RegisterableEventHandler {
    private final AffectKeyRegistry registry = new AffectKeyRegistry();
    private final SingleLdtpEngine engine;
    private final SocketServer server;

    public SingleHandler(SingleLdtpEngine singleLdtpEngine, SocketServer socketServer) {
        engine = singleLdtpEngine;
        server = socketServer;
    }

    @Override
    protected void handleClientRequest(SocketRequest request) {
        engine.offerInterruptibly(request);
    }

    @Override
    protected void handleRegisterKey(SocketRequest request) {
        byte[] data = request.data();
        QueryParams params = QueryParams.parse(data);

        if(params.method() != Request.REGISTER_KEY) {
            throw new RuntimeException();
        }

        registry.register(request.selectionKey(), null);
    }

    @Override
    protected void handleDeregisterKey(SocketRequest request) {
        byte[] data = request.data();
        QueryParams params = QueryParams.parse(data);

        if(params.method() != Request.DEREGISTER_KEY) {
            throw new RuntimeException();
        }

        registry.deregister(request.selectionKey(), null);
    }

    @Override
    public void handleResponse(WritableSocketResponse response) {

    }
}
