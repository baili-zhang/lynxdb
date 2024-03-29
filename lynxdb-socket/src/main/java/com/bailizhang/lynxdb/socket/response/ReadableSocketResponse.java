/*
 * Copyright 2022-2023 Baili Zhang.
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
import com.bailizhang.lynxdb.socket.interfaces.Readable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class ReadableSocketResponse extends SocketResponse implements Readable {
    private final ByteBuffer lengthBuffer;
    private final ByteBuffer serialBuffer;
    private ByteBuffer dataBuffer;


    public ReadableSocketResponse(SelectionKey key) {
        super(key);
        lengthBuffer = ByteBuffer.allocate(INT_LENGTH);
        serialBuffer = ByteBuffer.allocate(INT_LENGTH);
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!BufferUtils.isOver(lengthBuffer)) {
            channel.read(lengthBuffer);
            if(!BufferUtils.isOver(lengthBuffer)) {
                return;
            }
            int len = lengthBuffer.getInt(0);
            dataBuffer = ByteBuffer.allocate(len - INT_LENGTH);
        }

        if(!BufferUtils.isOver(serialBuffer)) {
            channel.read(serialBuffer);
            if(!BufferUtils.isOver(serialBuffer)) {
                return;
            }
            serial = serialBuffer.getInt(0);
        }

        if(!BufferUtils.isOver(dataBuffer) || dataBuffer.capacity() == 0) {
            channel.read(dataBuffer);
            if(BufferUtils.isOver(dataBuffer)) {
                data = dataBuffer.array();
            }
        }
    }

    @Override
    public boolean isReadCompleted() {
        return BufferUtils.isOver(dataBuffer);
    }
}
