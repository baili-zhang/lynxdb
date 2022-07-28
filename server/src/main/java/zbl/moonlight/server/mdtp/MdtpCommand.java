package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;

import java.nio.channels.SelectionKey;

public class MdtpCommand {
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

    public long serial() {
        return 0L;
    }

    public String adapterName() {
        return null;
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
