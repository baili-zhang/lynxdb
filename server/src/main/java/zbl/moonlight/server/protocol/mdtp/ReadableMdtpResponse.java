package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.common.ReadStrategy;

public class ReadableMdtpResponse extends ReadStrategy {
    public ReadableMdtpResponse() {
        super(MdtpResponseSchema.class);
    }
}
