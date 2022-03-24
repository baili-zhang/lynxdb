package zbl.moonlight.server.config;

import java.util.LinkedHashMap;
import java.util.List;

public record ClusterConfiguration(List<LinkedHashMap<String, Object>> nodes) {
}
