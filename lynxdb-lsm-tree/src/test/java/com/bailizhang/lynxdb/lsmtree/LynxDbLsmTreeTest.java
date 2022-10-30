package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.common.LsmTree;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.awt.desktop.OpenFilesEvent;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class LynxDbLsmTreeTest {
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data";

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    @Test
    void insert() {
        Options options = new Options();
        options.createColumnFamilyIfNotExisted(true);

        LsmTree lsmTree = new LynxDbLsmTree(BASE_DIR, options);
        lsmTree.insert(
                "key".getBytes(),
                "columnFamily".getBytes(),
                "column".getBytes(),
                System.currentTimeMillis(),
                "value".getBytes()
        );
    }
}