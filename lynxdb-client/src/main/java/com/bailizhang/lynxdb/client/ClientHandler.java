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

package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements SocketClientHandler {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> futureMap;

    public ClientHandler(
            ConcurrentHashMap<SelectionKey, ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> map
    ) {
        futureMap = map;
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) {
        futureMap.put(selectionKey, new ConcurrentHashMap<>());
    }

    @Override
    public void handleDisconnect(SelectionKey selectionKey) {
        var futures = futureMap.remove(selectionKey);
        futures.forEach((serial, future) -> future.cancel(false));
    }

    @Override
    public void handleBeforeSend(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(selectionKey);
        if(map == null) {
            map = new ConcurrentHashMap<>();
            futureMap.put(selectionKey, map);
        }
        map.put(serial, new LynxDbFuture<>());
    }

    @Override
    public void handleResponse(SocketResponse response) {
        int serial = response.serial();
        byte[] data = response.data();

        SelectionKey key = response.selectionKey();
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(key);

        if(map == null) {
            throw new RuntimeException("Map is null");
        }

        LynxDbFuture<byte[]> future = map.remove(serial);
        future.value(data);
    }
}
