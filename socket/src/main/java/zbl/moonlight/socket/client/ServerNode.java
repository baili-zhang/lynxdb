package zbl.moonlight.socket.client;

public record ServerNode (String host, int port) {
    @Override
    public String toString() {
        return host + ":" + port;
    }

    public static ServerNode parse(String node) {
        String[] info = node.split(":");

        if(info.length != 2) {
            throw new RuntimeException("Parse ServerNode failed.");
        }

        return new ServerNode(info[0], Integer.parseInt(info[1]));
    }
}
