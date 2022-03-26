package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.utils.ByteArrayUtils;

public class MdtpRequest {
    private final byte method;
    private final byte[] serial;
    private final byte[] key;
    private final byte[] value;

    public MdtpRequest(NioReader reader) {
        method = reader.mapGet(MdtpSchemaEntryName.METHOD)[0];
        serial = reader.mapGet(MdtpSchemaEntryName.SERIAL);
        key = reader.mapGet(MdtpSchemaEntryName.KEY);
        value = reader.mapGet(MdtpSchemaEntryName.VALUE);
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
        return "{" +
                MdtpSchemaEntryName.METHOD + ": " + MdtpMethod.getMethodName(method) + ", " +
                MdtpSchemaEntryName.SERIAL + ": " + ByteArrayUtils.toInt(serial) + ", " +
                MdtpSchemaEntryName.KEY + ": " + new String(key) + ", " +
                MdtpSchemaEntryName.VALUE + ": " + new String(value)
                + "}";
    }
}
