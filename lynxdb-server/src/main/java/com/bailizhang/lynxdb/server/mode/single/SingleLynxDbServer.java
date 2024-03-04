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

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleLynxDbServer implements LynxDbServer {
    private static final Logger logger = LoggerFactory.getLogger(SingleLynxDbServer.class);

    private final SocketServer server;
    private final LdtpEngineExecutor engineExecutor;

    public SingleLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        SocketServerConfig serverConfig = new SocketServerConfig(current.port());
        server = new SocketServer(serverConfig);
        engineExecutor = new LdtpEngineExecutor(server);

        SingleHandler handler = new SingleHandler(engineExecutor);
        server.setHandler(handler);
    }

    @Override
    public void run() {
        logger.info("Run LynxDB single server.");

        Executor.start(server);
        Executor.start(engineExecutor);
    }
}
