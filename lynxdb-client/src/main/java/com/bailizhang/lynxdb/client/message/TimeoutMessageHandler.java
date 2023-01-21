package com.bailizhang.lynxdb.client.message;

import com.bailizhang.lynxdb.server.engine.message.MessageType;

public abstract class TimeoutMessageHandler extends MessageHandler {
    @Override
    public boolean canHandle(byte[] msg) {
        return msg.length > 1 && msg[0] == MessageType.TIMEOUT;
    }
}
