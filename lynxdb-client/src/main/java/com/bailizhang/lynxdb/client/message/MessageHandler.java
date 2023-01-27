package com.bailizhang.lynxdb.client.message;

import java.nio.ByteBuffer;

public interface MessageHandler {
    void doHandle(ByteBuffer buffer);
}
