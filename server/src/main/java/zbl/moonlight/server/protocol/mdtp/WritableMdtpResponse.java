package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.common.WriteStrategy;

public class WritableMdtpResponse extends WriteStrategy {
    public WritableMdtpResponse() {
        super(MdtpResponseSchema.class);
    }
}
