package com.bailizhang.lynxdb.client.message;

public class MessageHandlerChain {
    private MessageHandler head;
    private MessageHandler tail;

    public void next(MessageHandler handler) {
        if(tail == null) {
            tail = handler;
            head = handler;
            return;
        }

        tail.next(handler);
        tail = handler;
    }

    protected void handle(byte[] msg) {
        head.handle(msg);
    }
}
