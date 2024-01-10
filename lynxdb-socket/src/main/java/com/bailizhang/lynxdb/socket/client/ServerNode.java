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

package com.bailizhang.lynxdb.socket.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public record ServerNode (String host, int port) {
    private static final String SEPARATOR = ":";
    private static final String DELIMITER = ",";

    @Override
    public String toString() {
        return host + SEPARATOR + port;
    }

    public static ServerNode from(String node) {
        String[] info = node.split(SEPARATOR);

        if(info.length != 2) {
            throw new RuntimeException("Parse ServerNode failed.");
        }

        return new ServerNode(info[0], Integer.parseInt(info[1]));
    }

    public static byte[] nodesToBytes(Collection<ServerNode> currentNodes) {
        String total = currentNodes.stream().map(ServerNode::toString)
                .collect(Collectors.joining(DELIMITER));
        return total.getBytes(StandardCharsets.UTF_8);
    }

    public static List<ServerNode> parseNodeList(String value) {
        if(value == null) {
            return new ArrayList<>();
        }

        String[] nodes = value.trim().split(DELIMITER);
        return Arrays.stream(nodes).map(ServerNode::from).toList();
    }

    public static List<ServerNode> parseNodeList(byte[] value) {
        if(value == null) {
            return new ArrayList<>();
        }

        String total = new String(value);
        return parseNodeList(total);
    }
}
