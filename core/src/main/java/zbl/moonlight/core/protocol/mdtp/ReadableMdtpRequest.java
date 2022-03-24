package zbl.moonlight.core.protocol.mdtp;

import zbl.moonlight.core.protocol.common.ReadStrategy;
import zbl.moonlight.core.utils.ByteArrayUtils;

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

    @Override
    public String toString() {
        return "{" +
                MdtpSchema.METHOD + ": " + MdtpMethod.getMethodName(method()) + ", " +
                MdtpSchema.SERIAL + ": " + ByteArrayUtils.toInt(serial()) + ", " +
                MdtpSchema.KEY + ": " + new String(key()) + ", " +
                MdtpSchema.VALUE + ": " + new String(value())
                + "}";
    }
}
