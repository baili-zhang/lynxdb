package com.bailizhang.lynxdb.core.mmap;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class MappedBufferTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/logs";
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
    void get() {
        mappedBuffer.get();
    }
}