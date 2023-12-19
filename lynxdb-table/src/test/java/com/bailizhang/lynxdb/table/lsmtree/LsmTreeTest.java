package com.bailizhang.lynxdb.table.lsmtree;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

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