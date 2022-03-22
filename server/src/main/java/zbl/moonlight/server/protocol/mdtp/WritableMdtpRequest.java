package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.common.WriteStrategy;

public class WritableMdtpRequest extends WriteStrategy {
    public WritableMdtpRequest() {
        super(MdtpRequestSchema.class);
    }
}
