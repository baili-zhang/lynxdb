package com.bailizhang.lynxdb.core.arena;

import org.junit.jupiter.api.Test;

class ArenaTest {
    @Test
    void test_001() {
        Arena arena = new Arena(1024, 256);

        ArenaBuffer[] buffers = new ArenaBuffer[4];
        for(int i = 0; i < 4; i ++) {
            buffers[i] = arena.alloc();
        }

        arena.dealloc(buffers[0]);
        arena.alloc();
    }

    @Test
    void test_002() {

    }
}