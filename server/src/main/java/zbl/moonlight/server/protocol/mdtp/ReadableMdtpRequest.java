package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.common.ReadStrategy;

public class ReadableMdtpRequest extends ReadStrategy {
    public ReadableMdtpRequest() {
        super(MdtpRequestSchema.class);
    }

    public byte method() {
        return mapGet(MdtpSchema.METHOD)[0];
    }

    public byte[] serial() {
        return mapGet(MdtpSchema.SERIAL);
    }

    public byte[] key() {
        return mapGet(MdtpSchema.KEY);
    }

    public byte[] value() {
        return mapGet(MdtpSchema.VALUE);
    }
}
