package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class LogGroupTest {
    private LogGroup logGroup;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        logGroup = new LogGroup();
    }

    @AfterEach
    void tearDown() {
        logGroup.delete();
    }

    @Test
    void appendKvDelete() {
    }

    @Test
    void appendKvSet() {
    }

    @Test
    void appendTableDelete() {
    }

    @Test
    void appendTableSet() {
    }
}