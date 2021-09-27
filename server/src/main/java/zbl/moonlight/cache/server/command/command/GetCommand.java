package zbl.moonlight.cache.server.command.command;

import zbl.moonlight.cache.server.command.Command;
import zbl.moonlight.cache.server.engine.simple.SimpleCache;

public class GetCommand extends Command {

    public GetCommand(String key) {
        super(key, null);
    }

    @Override
    public GetCommand exec() {
        value = SimpleCache.getInstance().get(key);
        return this;
    }

    @Override
    public String wrap() {
        if(value == null) {
            return "[Invalid Key]";
        }

        return "[OK] " + value;
    }
}
