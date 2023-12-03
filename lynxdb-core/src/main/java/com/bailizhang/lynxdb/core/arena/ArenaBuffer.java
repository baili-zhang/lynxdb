package com.bailizhang.lynxdb.core.arena;

import java.nio.ByteBuffer;

public record ArenaBuffer (
        int bit,
        ByteBuffer buffer
) {
}
