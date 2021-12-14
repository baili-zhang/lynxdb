package zbl.moonlight.server.command.commands;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.engine.simple.SimpleCache;

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
