package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.server.mode.AbstractLdtpEngine;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;

public class SingleLdtpEngine extends AbstractLdtpEngine {

    public SingleLdtpEngine(SocketServer socketServer, LynxDbTimeWheel lynxDbTimeWheel) {
        super(socketServer, lynxDbTimeWheel);
    }
}
