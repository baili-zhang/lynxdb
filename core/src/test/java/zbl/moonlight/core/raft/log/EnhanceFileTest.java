package zbl.moonlight.core.raft.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zbl.moonlight.core.utils.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class EnhanceFileTest {
    private EnhanceFile file;

    @BeforeEach
    void setUp() throws IOException {
        String filePath = System.getProperty("user.dir") + "/temp";
        String filename = "enhance_file_test.txt";
        file = new EnhanceFile(filePath, filename);
    }

    @AfterEach
    void tearDown() throws IOException {
        assert file.delete();
    }

    @Test
    void writeAndRead() throws IOException {
        file.write(ByteBuffer.wrap("hallo world".getBytes(StandardCharsets.UTF_8)),0);
        ByteBuffer buffer = ByteBuffer.allocate(5);
        file.read(buffer, 6);
        assert new String(buffer.array()).equals("world");
    }

    @Test
    void length() throws IOException {
        ByteBuffer intBuffer = ByteBufferUtils.intByteBuffer().putInt(30).rewind();
        assert file.length() == 0;
        file.write(intBuffer, 0);
        assert file.length() == 4;

        /* "hallo" 在 utf-8 编码下占五个字节 */
        byte[] data = "hallo".getBytes(StandardCharsets.UTF_8);
        ByteBuffer stringBuffer = ByteBuffer.wrap(data).rewind();
        file.write(stringBuffer, 0);
        assert file.length() == 5;
    }
}