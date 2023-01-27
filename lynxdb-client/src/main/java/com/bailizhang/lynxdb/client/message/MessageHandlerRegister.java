package com.bailizhang.lynxdb.client.message;

import com.bailizhang.lynxdb.server.engine.message.MessageKey;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.bailizhang.lynxdb.server.engine.message.MessageType.AFFECT;
import static com.bailizhang.lynxdb.server.engine.message.MessageType.TIMEOUT;

public class MessageHandlerRegister {

    private final HashMap<MessageKey, List<MessageHandler>> affectHandlers = new HashMap<>();
    private final HashMap<MessageKey, List<MessageHandler>> timeoutHandlers = new HashMap<>();

    public void handle(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte type = buffer.get();

        MessageKey messageKey = MessageKey.from(buffer);

        switch (type) {
            case AFFECT -> {
                List<MessageHandler> handlers = affectHandlers.get(messageKey);
                handle(handlers, messageKey, buffer);
            }

            case TIMEOUT -> {
                List<MessageHandler> handlers = timeoutHandlers.get(messageKey);
                handle(handlers, messageKey, buffer);
            }

            default -> throw new RuntimeException();
        }
    }

    public void registerAffectHandler(MessageKey messageKey, MessageHandler messageHandler) {
        List<MessageHandler> handlers = affectHandlers.computeIfAbsent(messageKey, k -> new ArrayList<>());
        handlers.add(messageHandler);
    }

    public void registerTimeoutHandler(MessageKey messageKey, MessageHandler messageHandler) {
        List<MessageHandler> handlers = timeoutHandlers.computeIfAbsent(messageKey, k -> new ArrayList<>());
        handlers.add(messageHandler);
    }

    private void handle(List<MessageHandler> handlers, MessageKey messageKey, ByteBuffer buffer) {
        int limit = buffer.limit();
        int position = buffer.position();

        for(MessageHandler handler : handlers) {
            handler.doHandle(messageKey, buffer);

            buffer.limit(limit);
            buffer.position(position);
        }
    }

}
