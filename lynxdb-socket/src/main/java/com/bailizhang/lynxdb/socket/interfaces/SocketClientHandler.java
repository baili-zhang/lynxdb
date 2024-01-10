/*
 * Copyright 2022 Baili Zhang.
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

package com.bailizhang.lynxdb.socket.interfaces;

import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;

public interface SocketClientHandler {
    default void handleConnected(SelectionKey selectionKey) throws Exception {}
    default void handleAfterLatchAwait() throws Exception {}
    default void handleBeforeSend(SelectionKey selectionKey, int serial) {}
    default void handleResponse(SocketResponse response) throws Exception {}
    default void handleConnectFailure(SelectionKey selectionKey) throws Exception {}
    default void handleDisconnect(SelectionKey selectionKey) throws Exception {}
}
