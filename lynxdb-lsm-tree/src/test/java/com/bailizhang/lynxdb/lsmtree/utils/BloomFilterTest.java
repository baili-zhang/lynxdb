package com.bailizhang.lynxdb.lsmtree.utils;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {
    private static final String DIR = System.getProperty("user.dir") + "/temp";
    private static final String BLOOM_FILTER_NAME = "bloom_filter";
    private static final Path BLOOM_FILTER_PATH = Path.of(DIR, BLOOM_FILTER_NAME);


    private BloomFilter bloomFilter;
    private FileChannel fileChannel;

    @BeforeEach
    void setUp() {
        FileUtils.createDirIfNotExisted(DIR);
        FileUtils.createFileIfNotExisted(BLOOM_FILTER_PATH.toFile());

        fileChannel = FileChannelUtils.open(
                BLOOM_FILTER_PATH,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );

        bloomFilter = new BloomFilter(fileChannel, 2000);
    }

    @AfterEach
    void tearDown() throws IOException {
        fileChannel.close();
        FileUtils.delete(BLOOM_FILTER_PATH);
    }

    @Test
    void test01() {
        bloomFilter.setObj("hallo");
        assert bloomFilter.isExist("hallo");
        assert !bloomFilter.isExist("hello");
    }
}