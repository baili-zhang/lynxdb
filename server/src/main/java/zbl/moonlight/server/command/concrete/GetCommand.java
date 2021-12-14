package zbl.moonlight.server.command.concrete;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.annotation.MoonlightCommand;
import zbl.moonlight.server.engine.simple.SimpleCache;

@MoonlightCommand("get")
public class GetCommand extends Command {
    @Override
    public GetCommand exec() {
        setValue(SimpleCache.getInstance().get(getKey()));
        return this;
    }

    @Override
    public String wrap() {
        if(getValue() == null) {
            return "[Invalid Key]";
        }

        return "[OK] " + getValue();
    }
}
