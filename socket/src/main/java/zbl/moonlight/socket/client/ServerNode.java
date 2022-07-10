package zbl.moonlight.socket.client;

public record ServerNode (String host, int port) {
    @Override
    public String toString() {
        return host + ":" + port;
    }
}
