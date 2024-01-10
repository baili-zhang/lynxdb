/*
 * Copyright 2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
