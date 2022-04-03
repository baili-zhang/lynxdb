package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.utils.ByteArrayUtils;

public class MdtpResponse {
    private final byte status;
    private final int serial;
    private final byte[] value;

    public MdtpResponse(NioReader reader) {
        status = reader.mapGet(MdtpResponseSchema.STATUS)[0];
        serial = ByteArrayUtils.toInt(reader.mapGet(MdtpResponseSchema.SERIAL));
        value = reader.mapGet(MdtpResponseSchema.VALUE);
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
                "status: " + ResponseStatus.getCodeName(status) + ", " +
                "serial: " + serial + ", " +
                "value: " + new String(value) + "}";
    }
}
