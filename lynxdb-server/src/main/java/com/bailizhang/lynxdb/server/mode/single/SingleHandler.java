package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.server.engine.AffectKey;
import com.bailizhang.lynxdb.server.engine.AffectValue;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.server.engine.query.RegisterKeyContent;
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

        RegisterKeyContent content = new RegisterKeyContent(params);
        registry.register(request.selectionKey(), content.affectKey());
    }

    @Override
    protected void handleDeregisterKey(SocketRequest request) {
        byte[] data = request.data();
        QueryParams params = QueryParams.parse(data);

        if(params.method() != Request.DEREGISTER_KEY) {
            throw new RuntimeException();
        }

        RegisterKeyContent content = new RegisterKeyContent(params);
        registry.deregister(request.selectionKey(), content.affectKey());
    }

    @Override
    public void handleResponse(WritableSocketResponse response) {
        AffectValue affectValue = (AffectValue)response.extraData();

        // 防止循环发送很多 AffectValue 消息
        if(affectValue == null) {
            return;
        }

        AffectKey affectKey = affectValue.affectKey();
        List<SelectionKey> selectionKeys = registry.selectionKeys(affectKey);

        for(SelectionKey selectionKey : selectionKeys) {
            // extraData 设置为 null，防止循环发送很多 AffectValue 消息
            WritableSocketResponse socketResponse = new WritableSocketResponse(
                    selectionKey,
                    -1,
                    affectValue,
                    null
            );
            server.offerInterruptibly(socketResponse);
        }
    }
}
