package com.bailizhang.lynxdb.socket.client;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public record ServerNode (String host, int port) {
    private static final String SEPARATOR = ":";

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

    public static byte[] nodeListToBytes(List<ServerNode> currentNodes) {
        String total = currentNodes.stream().map(ServerNode::toString)
                .collect(Collectors.joining(" "));
        return total.getBytes(StandardCharsets.UTF_8);
    }

    public static List<ServerNode> parseNodeList(byte[] value) {
        if(value == null) {
            return new ArrayList<>();
        }

        String total = new String(value);
        String[] nodes = total.trim().split("\\s+");
        return Arrays.stream(nodes).map(ServerNode::from).toList();
    }
}
