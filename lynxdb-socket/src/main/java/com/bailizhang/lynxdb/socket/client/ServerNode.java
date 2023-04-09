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
