package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.common.WriteStrategy;
import zbl.moonlight.server.utils.ByteArrayUtils;

public class WritableMdtpResponse extends WriteStrategy {
    public WritableMdtpResponse() {
        super(MdtpResponseSchema.class);
    }

    public String toString() {
        return "{" +
                MdtpSchema.METHOD + ": " + ResponseStatus.getCodeName(map.get(MdtpSchema.STATUS)[0]) + ", " +
                MdtpSchema.SERIAL + ": " + ByteArrayUtils.toInt(map.get(MdtpSchema.SERIAL)) + ", " +
                MdtpSchema.VALUE + ": " + new String(map.get(MdtpSchema.VALUE))
                + "}";
    }
}
