package com.bailizhang.lynxdb.core.arena;

import com.bailizhang.lynxdb.core.arena.exceptions.ArenaOverflowException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ArenaTest {
    @Test
    void test_001() throws ArenaOverflowException {
        Arena arena = new Arena(1024, 256);

        ArenaBuffer[] buffers = new ArenaBuffer[4];
        for(int i = 0; i < 4; i ++) {
            buffers[i] = arena.alloc();
        }

        arena.dealloc(buffers[0]);
        arena.alloc();
    }

    @Test
    void test_002() throws ArenaOverflowException {
        Arena arena = new Arena(1024, 256);

        for(int i = 0; i < 4; i ++) {
            arena.alloc();
        }

        Assertions.assertThrows(ArenaOverflowException.class, arena::alloc);
    }
}