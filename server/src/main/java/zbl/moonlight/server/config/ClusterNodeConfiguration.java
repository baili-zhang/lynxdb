package zbl.moonlight.server.config;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class ClusterNodeConfiguration {
    private final String host;
    private final int port;

    ClusterNodeConfiguration(String host, int port) {
        this.host = host;
        this.port = port;
    }
}
