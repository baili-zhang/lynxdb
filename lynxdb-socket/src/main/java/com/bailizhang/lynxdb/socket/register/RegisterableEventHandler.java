package com.bailizhang.lynxdb.socket.register;

import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

import static com.bailizhang.lynxdb.socket.code.Request.*;

public abstract class RegisterableEventHandler implements SocketServerHandler {
    public final void handleRequest(SocketRequest request) {
        byte[] data = request.data();
        if(data == null || data.length < 1) {
            return;
        }

        switch (data[0]) {
            case CLIENT_REQUEST -> handleClientRequest(request);
            case REGISTER_KEY -> handleRegisterKey(request);
            case DEREGISTER_KEY -> handleDeregisterKey(request);
        }
    }

    protected void handleClientRequest(SocketRequest request) {

    }

    protected void handleRegisterKey(SocketRequest request) {

    }

    protected void handleDeregisterKey(SocketRequest request) {
    }
}
