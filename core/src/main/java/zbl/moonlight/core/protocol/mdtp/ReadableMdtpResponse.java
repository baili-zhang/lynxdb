package zbl.moonlight.core.protocol.mdtp;

import zbl.moonlight.core.protocol.common.ReadStrategy;

public class ReadableMdtpResponse extends ReadStrategy {
    public ReadableMdtpResponse() {
        super(MdtpResponseSchema.class);
    }
}
