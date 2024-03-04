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

package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;

public class LynxDbRaftConfiguration implements RaftConfiguration {
    private final List<ServerNode> initClusterMembers;
    private final ServerNode currentNode;
    private final String logsDir;
    private final String metaDir;

    public LynxDbRaftConfiguration() {
        String members = Configuration.getInstance().initClusterMembers();

        initClusterMembers = ServerNode.parseNodeList(members);
        currentNode = Configuration.getInstance().currentNode();
        logsDir = Configuration.getInstance().raftLogsDir();
        metaDir = Configuration.getInstance().raftMetaDir();
    }

    @Override
    public List<ServerNode> initClusterMembers() {
        return initClusterMembers;
    }

    @Override
    public ServerNode currentNode() {
        return currentNode;
    }

    @Override
    public String logsDir() {
        return logsDir;
    }

    @Override
    public String metaDir() {
        return metaDir;
    }
}
