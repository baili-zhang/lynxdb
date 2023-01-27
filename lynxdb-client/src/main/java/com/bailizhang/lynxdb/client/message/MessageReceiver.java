package com.bailizhang.lynxdb.client.message;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;

public class MessageReceiver extends Executor<byte[]> {
    private final MessageHandlerRegister register;

    public MessageReceiver() {
        register = new MessageHandlerRegister();
    }

    @Override
    protected void execute() {
        byte[] data = blockPoll();

        if(data != null) {
            register.handle(data);
        }
    }

    public void registerAffectHandler(MessageKey messageKey, MessageHandler messageHandler) {
        register.registerAffectHandler(messageKey, messageHandler);
    }

    public void registerTimeoutHandler(MessageKey messageKey, MessageHandler messageHandler) {
        register.registerTimeoutHandler(messageKey, messageHandler);
    }
}
