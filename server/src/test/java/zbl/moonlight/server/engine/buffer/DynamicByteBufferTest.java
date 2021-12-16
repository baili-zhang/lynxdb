package zbl.moonlight.server.engine.buffer;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class DynamicByteBufferTest {
    DynamicByteBuffer createDynamicByteBuffer(String string) {
        DynamicByteBuffer dynamicByteBuffer = new DynamicByteBuffer(10);
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        ByteBuffer chunk = dynamicByteBuffer.last();

        for(int i = 0, index = 0; i < bytes.length; i ++, index ++) {
            chunk.put(bytes[i]);
            if(chunk.position() == chunk.limit()) {
                chunk = dynamicByteBuffer.last();
                index = 0;
            }
        }

        return dynamicByteBuffer;
    }

    String dynamicByteBufferToString(DynamicByteBuffer dynamicByteBuffer) {
        byte[] resultBytes = new byte[dynamicByteBuffer.size()];
        int index = 0, chunkSize = dynamicByteBuffer.getChunkSize();
        for(int i = 0; i < dynamicByteBuffer.size(); i ++) {
            if((i - index * chunkSize) == chunkSize) {
                index ++;
            }
            resultBytes[i] = dynamicByteBuffer.get(index).get(i % chunkSize);
        }

        return new String(resultBytes);
    }

    @Test
    void size() {
        String string = "";
        DynamicByteBuffer dynamicByteBuffer = createDynamicByteBuffer(string);
        assert dynamicByteBuffer.size() == string.getBytes(StandardCharsets.UTF_8).length;

        String s1 = "assert dynamicByteBuffer1.size() == string.length();";
        DynamicByteBuffer dynamicByteBuffer1 = createDynamicByteBuffer(s1);
        assert dynamicByteBuffer1.size() == s1.getBytes(StandardCharsets.UTF_8).length;

        String s2 = "DynamicByteBuffer dynamicByteBuffer = createDynamicByteBuffer(string);";
        DynamicByteBuffer dynamicByteBuffer2 = createDynamicByteBuffer(s2);
        assert dynamicByteBuffer2.size() == s2.getBytes(StandardCharsets.UTF_8).length;
    }

    @Test
    void copyFrom() {
        String str = "DynamicByteBuffer dynamicByteBuffer = createDynamicByteBuffer(string);";
        DynamicByteBuffer src = createDynamicByteBuffer(str);
        DynamicByteBuffer dst = new DynamicByteBuffer(20);
        dst.copyFrom(src, 7, 40);
        assert dst.size() == 40;
        assert "ByteBuffer dynamicByteBuffer = createDyn".equals(dynamicByteBufferToString(dst));

        DynamicByteBuffer dst2 = new DynamicByteBuffer(15);
        dst2.copyFrom(src, 0, 20);
        assert dst2.size() == 20;
        assert "DynamicByteBuffer dy".equals(dynamicByteBufferToString(dst2));
    }

    @Test
    void copyTo() {
        String str = "DynamicByteBuffer dynamicByteBuffer = createDynamicByteBuffer(string);";
        DynamicByteBuffer src = createDynamicByteBuffer(str);
        ByteBuffer dst = ByteBuffer.allocate(20);
        src.copyTo(dst, 0, 20);
        assert "DynamicByteBuffer dy".equals(new String(dst.array()));
    }
}