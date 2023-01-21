package com.bailizhang.lynxdb.client.message;

public abstract class MessageHandler {
    private MessageHandler next;

    public void next(MessageHandler handler) {
        next = handler;
    }

    public void handle(byte[] msg) {
        if(canHandle(msg)) {
            doHandle(msg);
            return;
        }

        if(next != null) {
            next.handle(msg);
        }
    }

    public abstract void doHandle(byte[] msg);

    public abstract boolean canHandle(byte[] msg);
}
