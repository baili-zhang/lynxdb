package rcache.reactor;

import lombok.Data;

@Data
public class Handle {
    private EventType type;
    private EventHandler eventHandler;
}
