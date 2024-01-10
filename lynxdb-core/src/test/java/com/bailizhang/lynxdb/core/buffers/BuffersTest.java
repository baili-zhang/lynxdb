/*
 * Copyright 2024 Baili Zhang.
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

package com.bailizhang.lynxdb.core.buffers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

class BuffersTest {

    private Buffers buffers;

    @BeforeEach
    void setUp() {
        ByteBuffer[] rawBuffers = new ByteBuffer[2];
        rawBuffers[0] = ByteBuffer.allocate(3);
        rawBuffers[1] = ByteBuffer.allocate(10);

        rawBuffers[1].put(0, (byte)0x09);

        buffers = new Buffers(rawBuffers);
        buffers.rewind();
    }

    @Test
    void getVar() {
        assert buffers.getInt() == 9;
        byte[] data = buffers.getVar(9);
        assert data.length == 9;
        assert !buffers.hasRemaining();
    }
}