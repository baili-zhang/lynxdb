package zbl.moonlight.server.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class MdtpTest {

    @Test
    void encode() throws EncodeException {
        String key = "ByteBuffer byteBuffer";
        String value = "ByteBuffer.wrap(\"abc\".getBytes(StandardCharsets.UTF_8))";
        ByteBuffer byteBuffer = Mdtp.encode(MdtpMethod.GET, ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));

        assert byteBuffer.get(0) == MdtpMethod.GET;

        int keyLength = byteBuffer.get(1) & 0xff;
        int valueLength = ((byteBuffer.get(2) & 0xff) << 24) |
                ((byteBuffer.get(3) & 0xff) << 16) |
                ((byteBuffer.get(4) & 0xff) << 8) |
                (byteBuffer.get(5) & 0xff);

        assert keyLength == key.length();
        assert valueLength == value.length();

        byte[] keyBytes = new byte[keyLength];
        for (int i = Mdtp.HEADER_LENGTH; i < Mdtp.HEADER_LENGTH + keyLength; i ++) {
            keyBytes[i-Mdtp.HEADER_LENGTH] = byteBuffer.get(i);
        }
        assert key.equals(new String(keyBytes));

        byte[] valueBytes = new byte[valueLength];
        int valueOffset = Mdtp.HEADER_LENGTH + keyLength;
        for (int i = valueOffset; i < valueOffset + valueLength; i ++) {
            valueBytes[i-valueOffset] = byteBuffer.get(i);
        }
        assert value.equals(new String(valueBytes));
    }

    @Test
    void decode() {
    }
}