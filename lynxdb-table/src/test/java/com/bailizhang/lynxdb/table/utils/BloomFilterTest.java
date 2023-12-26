/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.table.utils;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

class BloomFilterTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/bloom_filter_test";

    private BloomFilter bloomFilter;

    @BeforeEach
    void setUp() {
        Path filePath = Path.of(BASE_DIR);
        FileUtils.createFile(filePath);
        bloomFilter = BloomFilter.from(filePath, 0, 1000);
    }

    @AfterEach
    void tearDown() {
        FileUtils.delete(Path.of(BASE_DIR));
    }

    @Test
    void test01() {
        bloomFilter.setObj("hallo".getBytes(StandardCharsets.UTF_8));
        assert bloomFilter.isExist("hallo".getBytes(StandardCharsets.UTF_8));
        assert !bloomFilter.isExist("hello".getBytes(StandardCharsets.UTF_8));
    }
}