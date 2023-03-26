package com.bailizhang.lynxdb.lsmtree.utils;

import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class BloomFilterTest {
    private BloomFilter bloomFilter;

    @BeforeEach
    void setUp() {
        // bloomFilter = new BloomFilter(2000);
    }

    @Test
    void test01() {
        bloomFilter.setObj("hallo".getBytes(StandardCharsets.UTF_8));
        assert bloomFilter.isExist("hallo".getBytes(StandardCharsets.UTF_8));
        assert !bloomFilter.isExist("hello".getBytes(StandardCharsets.UTF_8));
    }
}