package com.bailizhang.lynxdb.lsmtree.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BloomFilterTest {
    private BloomFilter bloomFilter;

    @BeforeEach
    void setUp() {
        bloomFilter = new BloomFilter(2000);
    }

    @Test
    void test01() {
        bloomFilter.setObj("hallo");
        assert bloomFilter.isExist("hallo");
        assert !bloomFilter.isExist("hello");
    }
}