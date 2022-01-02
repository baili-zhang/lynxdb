package zbl.moonlight.server.context;

import lombok.Getter;
import lombok.Setter;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.eventbus.EventBus;

public class ServerContext {
    private static final ServerContext INSTANCE = new ServerContext();

    @Getter
    @Setter
    private Engine engine;

    @Getter
    @Setter
    private EventBus eventBus;

    private ServerContext () {

    }

    public static ServerContext getInstance() {
        return INSTANCE;
    }
}
