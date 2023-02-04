package com.bailizhang.lynxdb.client.message;


import com.bailizhang.lynxdb.ldtp.message.MessageKey;

import java.nio.ByteBuffer;

public interface MessageHandler {
    void doHandle(MessageKey messageKey, ByteBuffer buffer);
}
