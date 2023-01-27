package com.bailizhang.lynxdb.client.message;

import com.bailizhang.lynxdb.server.engine.message.MessageKey;

import java.nio.ByteBuffer;

public interface MessageHandler {
    void doHandle(MessageKey messageKey, ByteBuffer buffer);
}
