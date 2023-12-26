/*
 * Copyright 2023 Baili Zhang.
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

package com.bailizhang.lynxdb.table.lsmtree;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

class LsmTreeTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data/lsmtree_test";

    private LsmTree lsmTree;

    @BeforeEach
    void setUp() {
        LsmTreeOptions options = new LsmTreeOptions(BASE_DIR, 200);
        lsmTree = new LsmTree(options);
    }

    @AfterEach
    void tearDown() {
        FileUtils.delete(Path.of(BASE_DIR));
    }

    @Test
    void find() {

    }

    @Test
    void insert() {
    }

    @Test
    void delete() {
    }

    @Test
    void existKey() {
    }

    @Test
    void rangeNext() {
    }

    @Test
    void rangeBefore() {
    }
}