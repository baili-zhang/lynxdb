package com.bailizhang.lynxdb.core.mmap;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;

class MappedBufferTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/temp";
    private static final String FILENAME = "MappedBufferTest.log";
    private MappedBuffer mappedBuffer;

    @BeforeEach
    void setUp() {
        FileUtils.createDirIfNotExisted(BASE_DIR);
        Path filePath = Path.of(BASE_DIR, FILENAME);
        FileUtils.createFileIfNotExisted(filePath.toFile());

        mappedBuffer = new MappedBuffer(filePath, 0, 1000);
    }

    @AfterEach
    void tearDown() {
        FileUtils.delete(Path.of(BASE_DIR));
    }

    @Test
    void testGC() {
        // 测试时需要改成弱引用
        func();
        System.gc();
        MappedByteBuffer nextBuffer = mappedBuffer.getBuffer();
        assert nextBuffer.position() != 0;
    }

    void func() {
        MappedByteBuffer buffer = mappedBuffer.getBuffer();
        buffer.put(new byte[]{0x01});
        System.gc();
        mappedBuffer.saveSnapshot(buffer);
    }
}