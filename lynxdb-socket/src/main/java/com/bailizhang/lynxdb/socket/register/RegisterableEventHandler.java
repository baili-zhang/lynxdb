package com.bailizhang.lynxdb.socket.register;

import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

import static com.bailizhang.lynxdb.socket.code.Request.CLIENT_REQUEST;
import static com.bailizhang.lynxdb.socket.code.Request.EVENT_REGISTER;

public abstract class RegisterableEventHandler implements SocketServerHandler {
    public final void handleRequest(SocketRequest request) {
        byte[] data = request.data();
        if(data == null || data.length < 1) {
            return;
        }

        switch (data[0]) {
            case CLIENT_REQUEST -> handleClientRequest(request);
            case EVENT_REGISTER -> handleEventRegister(request);
        }
    }

    protected void handleClientRequest(SocketRequest request) {

    }

    protected void handleEventRegister(SocketRequest request) {

    }
}
