package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.utils.ByteArrayUtils;

public class MdtpRequest {
    private final byte method;
    private final byte[] serial;
    private final byte[] key;
    private final byte[] value;

    public MdtpRequest(NioReader reader) {
        method = reader.mapGet(MdtpRequestSchema.METHOD)[0];
        serial = reader.mapGet(MdtpRequestSchema.SERIAL);
        key = reader.mapGet(MdtpRequestSchema.KEY);
        value = reader.mapGet(MdtpRequestSchema.VALUE);
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
