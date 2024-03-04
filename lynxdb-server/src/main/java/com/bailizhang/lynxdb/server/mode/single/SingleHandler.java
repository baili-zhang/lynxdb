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

package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SegmentSocketRequest;

public class SingleHandler implements SocketServerHandler {
    private final LdtpEngineExecutor engineExecutor;

    public SingleHandler(LdtpEngineExecutor executor) {
        engineExecutor = executor;
    }

    @Override
    public void handleRequest(SegmentSocketRequest request) {
        engineExecutor.offerInterruptibly(request);
    }
}
