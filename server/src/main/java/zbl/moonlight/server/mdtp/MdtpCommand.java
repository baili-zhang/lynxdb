package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;

import java.nio.channels.SelectionKey;

public class MdtpCommand {
    public static final byte SET = (byte) 0x01;
    public static final byte GET = (byte) 0x02;
    public static final byte DELETE = (byte) 0x03;

    private final SelectionKey selectionKey;
    private final byte method;
    private final byte[] key;
    private final byte[] value;

    public MdtpCommand(SelectionKey selectionKey, byte[] command) {
        this.selectionKey = selectionKey;
        EnhanceByteBuffer buffer = EnhanceByteBuffer.wrap(command);
        method = buffer.get();
        key = buffer.getBytes();
        value = buffer.getBytes();
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    public byte method() {
        return method;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }
}
