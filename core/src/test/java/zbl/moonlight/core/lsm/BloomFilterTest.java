package zbl.moonlight.core.lsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zbl.moonlight.core.enhance.EnhanceFile;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {
    private BloomFilter bloomFilter;
    private EnhanceFile file;
    private final String[] keys = new String[]{"BloomFilterTest", "testNew", "new BloomFilter(new byte[1000])"};

    @BeforeEach
    void beforeEach() throws IOException {
        file = new EnhanceFile(System.getProperty("user.dir") + "/temp", "bloom_filter.data");
        file.write(ByteBuffer.allocate(1000), 0);
        bloomFilter = new BloomFilter(file, 8000);
    }

    @AfterEach
    void afterEach() throws IOException {
        file.delete();
    }

    @Test
    void testFunc() {
        for (String key : keys) {
            bloomFilter.setKey(key);
            assert bloomFilter.isKeyExist(key);
        }
    }
}