package zbl.moonlight.server.command;

import lombok.Data;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.protocol.Mdtp;
import zbl.moonlight.server.response.Response;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

@Data
public class Command implements Cloneable {
    private SelectionKey selectionKey;

    private byte code;
    private ByteBuffer key;
    private DynamicByteBuffer value;

    private Response response;

    /**
     * should send response to client or not.
     */
    private boolean sendResponse;

    public Command() {
    }
}
