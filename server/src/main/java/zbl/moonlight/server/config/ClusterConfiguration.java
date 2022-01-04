package zbl.moonlight.server.config;

import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
public class ClusterConfiguration {
    private final List<ClusterNodeConfiguration> nodes;

    ClusterConfiguration(List<ClusterNodeConfiguration> nodes) {
        this.nodes = nodes;
    }
}
