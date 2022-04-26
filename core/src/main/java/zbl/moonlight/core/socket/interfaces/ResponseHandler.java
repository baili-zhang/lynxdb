package zbl.moonlight.core.socket.interfaces;

import zbl.moonlight.core.socket.response.ReadableSocketResponse;

public interface ResponseHandler {
    void handle(ReadableSocketResponse response);
}
