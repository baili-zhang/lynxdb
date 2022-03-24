package zbl.moonlight.core.protocol.mdtp;

import zbl.moonlight.core.protocol.common.WriteStrategy;

public class WritableMdtpRequest extends WriteStrategy {
    public WritableMdtpRequest() {
        super(MdtpRequestSchema.class);
    }
}
