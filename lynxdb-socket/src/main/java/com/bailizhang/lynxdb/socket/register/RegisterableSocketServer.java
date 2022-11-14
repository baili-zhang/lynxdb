package com.bailizhang.lynxdb.socket.register;

import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;

import java.io.IOException;

public class RegisterableSocketServer extends SocketServer {
    public RegisterableSocketServer(SocketServerConfig socketServerConfig) throws IOException {
        super(socketServerConfig);
    }

    @Override
    public void setHandler(SocketServerHandler handler) {
        if(handler instanceof RegisterableEventHandler) {
            super.setHandler(handler);
        }
    }
}
