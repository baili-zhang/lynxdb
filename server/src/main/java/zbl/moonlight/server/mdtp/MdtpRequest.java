package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.utils.ByteArrayUtils;

import java.nio.channels.SelectionKey;

public class MdtpRequest {
    private final SelectionKey selectionKey;

    private final byte method;
    private final byte[] serial;
    private final byte[] key;
    private final byte[] value;

    public MdtpRequest(NioReader reader) {
        selectionKey = reader.getSelectionKey();
        method = reader.mapGet(MdtpRequestSchema.METHOD)[0];
        serial = reader.mapGet(MdtpRequestSchema.SERIAL);
        key = reader.mapGet(MdtpRequestSchema.KEY);
        value = reader.mapGet(MdtpRequestSchema.VALUE);
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    public byte method() {
        return method;
    }

    public byte[] serial() {
        return serial;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    @Override
    public String toString() {
        return "{method: " + MdtpMethod.getMethodName(method) + ", " +
                "serial: " + ByteArrayUtils.toInt(serial) + ", " +
                "key: " + new String(key) + ", " +
                "value: " + new String(value) + "}";
    }
}
