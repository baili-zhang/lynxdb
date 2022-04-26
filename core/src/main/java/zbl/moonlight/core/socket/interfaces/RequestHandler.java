package zbl.moonlight.core.socket.interfaces;

import zbl.moonlight.core.socket.request.ReadableSocketRequest;

@FunctionalInterface
public interface RequestHandler {
    void handle(ReadableSocketRequest request);
}
