package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.utils.ByteArrayUtils;

public class MdtpResponse {
    private final byte status;
    private final int serial;
    private final byte[] value;

    public MdtpResponse(NioReader reader) {
        status = reader.mapGet(MdtpSchemaEntryName.STATUS)[0];
        serial = ByteArrayUtils.toInt(reader.mapGet(MdtpSchemaEntryName.SERIAL));
        value = reader.mapGet(MdtpSchemaEntryName.VALUE);
    }

    public byte status() {
        return status;
    }

    public int serial() {
        return serial;
    }

    public byte[] value() {
        return value;
    }

    @Override
    public String toString() {
        return "{" +
                MdtpSchemaEntryName.STATUS + ": " + ResponseStatus.getCodeName(status) + ", " +
                MdtpSchemaEntryName.SERIAL + ": " + serial + ", " +
                MdtpSchemaEntryName.VALUE + ": " + new String(value)
                + "}";
    }
}
