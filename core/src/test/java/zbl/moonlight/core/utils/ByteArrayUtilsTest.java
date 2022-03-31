package zbl.moonlight.core.utils;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class ByteArrayUtilsTest {

    @Test
    void toInt() {
    }

    @Test
    void fromInt() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(20);
        int i = ByteArrayUtils.toInt(byteBuffer.array());
        assert i == 20;
    }
}