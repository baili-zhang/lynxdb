package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.common.ReadStrategy;

public class ReadableMdtpRequest extends ReadStrategy {
    public ReadableMdtpRequest() {
        super(MdtpRequestSchema.class);
    }

    public byte getMethod() {
        return mapGet("method")[0];
    }
}
