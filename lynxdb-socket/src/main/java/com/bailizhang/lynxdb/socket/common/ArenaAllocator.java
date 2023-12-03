package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.arena.Arena;
import com.bailizhang.lynxdb.core.arena.ArenaBuffer;

public class ArenaAllocator {
    private static final Arena arena = new Arena(1024 * 1024 * 1024, 1024 * 8);
    public static ArenaBuffer alloc() {
        return arena.alloc();
    }

    public static void dealloc(ArenaBuffer arenaBuffer) {
        arena.dealloc(arenaBuffer);
    }
}
