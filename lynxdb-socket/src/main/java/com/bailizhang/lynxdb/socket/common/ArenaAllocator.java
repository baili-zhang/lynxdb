package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.arena.Arena;
import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.core.arena.exceptions.ArenaOverflowException;

public class ArenaAllocator {
    public static final int ARENA_BUFFER_SIZE = 1024 * 8;
    private static final Arena arena = new Arena(1024 * 1024 * 1024, ARENA_BUFFER_SIZE);
    public static ArenaBuffer alloc() {
        try {
            return arena.alloc();
        } catch (ArenaOverflowException ignored) {
            // TODO 处理 Arena 溢出问题
            throw new RuntimeException();
        }
    }

    public static void dealloc(ArenaBuffer arenaBuffer) {
        arena.dealloc(arenaBuffer);
    }
}
