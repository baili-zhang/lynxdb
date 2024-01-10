/*
 * Copyright 2022-2024 Baili Zhang.
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

package com.bailizhang.lynxdb.socket.response;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketResponse extends NioMessage implements Writable {
    private final ByteBuffer[] buffers;

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer[] data
    ) {
        super(true, selectionKey);
        dataBlocks.appendRawInt(serial);
        dataBlocks.appendRawBuffers(data);

        buffers = dataBlocks.toBuffers();
    }

    @Override
    public void write() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!isWriteCompleted()) {
            channel.write(buffers);
        }
    }

    @Override
    public boolean isWriteCompleted() {
        return BufferUtils.isOver(buffers);
    }
}
