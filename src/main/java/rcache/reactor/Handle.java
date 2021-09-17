package rcache.reactor;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Handle {
    private EventHandler eventHandler;
    private EventType type;
}
