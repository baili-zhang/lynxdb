/*
 * Copyright 2021-2023 Baili Zhang.
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

package com.bailizhang.lynxdb.server.mode.cluster;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.ldtp.LdtpStateMachine;
import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClusterLynxDbServer implements LynxDbServer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterLynxDbServer.class);

    private final RaftServer raftServer;
    private final LdtpEngineExecutor engineExecutor;

    public ClusterLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        raftServer = new RaftServer(current);
        engineExecutor = new LdtpEngineExecutor(raftServer);

        LdtpStateMachine.engineExecutor(engineExecutor);
        LdtpStateMachine.raftServer(raftServer);
    }

    @Override
    public void run() {
        logger.info("Run LynxDB cluster server.");

        Executor.start(raftServer);
        Executor.start(engineExecutor);
    }
}
