package com.bailizhang.lynxdb.core.arena;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class ArenaBufferTest {
    private static final int LENGTH = 200;

    private ArenaBuffer arenaBuffer;

    @BeforeEach
    void setUp() {
        ByteBuffer buffer = ByteBuffer.allocateDirect(LENGTH);
        arenaBuffer = new ArenaBuffer(1, buffer);
    }

    @Test
    void isClear() {
        assert arenaBuffer.isClear();
    }

    @Test
    void alloc() {
        Segment segment = arenaBuffer.alloc(2, 5);
        assert !arenaBuffer.isClear();
        arenaBuffer.dealloc(segment);
        assert arenaBuffer.isClear();
    }
}