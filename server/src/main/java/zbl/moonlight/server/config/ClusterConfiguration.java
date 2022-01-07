package zbl.moonlight.server.config;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashMap;
import java.util.List;

@Getter
@ToString
public class ClusterConfiguration {
    private final List<LinkedHashMap<String, Object>> nodes;

    ClusterConfiguration(List<LinkedHashMap<String, Object>> nodes) {
        this.nodes = nodes;
    }
}
