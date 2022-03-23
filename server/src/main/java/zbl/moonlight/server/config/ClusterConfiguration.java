package zbl.moonlight.server.config;

import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashMap;
import java.util.List;

@Getter
@ToString
public record ClusterConfiguration(List<LinkedHashMap<String, Object>> nodes) {
}
