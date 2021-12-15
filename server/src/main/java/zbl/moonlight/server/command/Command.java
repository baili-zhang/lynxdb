package zbl.moonlight.server.command;

import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

@Data
public class Command implements Cloneable {
    private byte code;
    private SelectionKey selectionKey;
    private int keyLength;
    private ByteBuffer key;
    private long valueLength;
    private ByteBuffer value;
    private ByteBuffer response;

    Command(byte code) {
        this.code = code;
    }

    public static Command create(byte code) {
        return new Command(code);
    }
}
