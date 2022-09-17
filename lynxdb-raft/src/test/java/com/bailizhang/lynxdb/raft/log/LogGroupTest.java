package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

class LogGroupTest {
    private LogGroup logGroup;

    static class KvDelete implements BytesListConvertible {
        private final BytesList bytesList = new BytesList(false);

        @Override
        public BytesList toBytesList() {
            bytesList.appendVarStr("key");
            bytesList.appendVarStr("value");
            return bytesList;
        }
    }

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
        logGroup.appendKvDelete(new KvDelete());
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