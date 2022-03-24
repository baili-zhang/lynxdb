package zbl.moonlight.core.protocol.mdtp;

import zbl.moonlight.core.protocol.common.WriteStrategy;
import zbl.moonlight.core.utils.ByteArrayUtils;

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
