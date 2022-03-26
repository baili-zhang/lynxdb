package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.utils.ByteArrayUtils;

public class MdtpRequest {
    private NioReader reader;

    public MdtpRequest(NioReader reader) {
        this.reader = reader;
    }

    public byte method() {
        return reader.mapGet(MdtpSchemaEntryName.METHOD)[0];
    }

    public byte[] serial() {
        return reader.mapGet(MdtpSchemaEntryName.SERIAL);
    }

    public byte[] key() {
        return reader.mapGet(MdtpSchemaEntryName.KEY);
    }

    public byte[] value() {
        return reader.mapGet(MdtpSchemaEntryName.VALUE);
    }

    @Override
    public String toString() {
        return "{" +
                MdtpSchemaEntryName.METHOD + ": " + MdtpMethod.getMethodName(method()) + ", " +
                MdtpSchemaEntryName.SERIAL + ": " + ByteArrayUtils.toInt(serial()) + ", " +
                MdtpSchemaEntryName.KEY + ": " + new String(key()) + ", " +
                MdtpSchemaEntryName.VALUE + ": " + new String(value())
                + "}";
    }
}
