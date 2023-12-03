package com.bailizhang.lynxdb.core.arena;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class ArenaTest {

    @Test
    void allocByteBuffer() {
        Arena arena = new Arena(1024, 256);
        for(int i = 0; i < 4; i ++) {
            arena.alloc();
        }
    }
}