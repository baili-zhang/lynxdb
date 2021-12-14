package zbl.moonlight.server.command.concrete;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.annotation.MoonlightCommand;
import zbl.moonlight.server.engine.simple.SimpleCache;

@MoonlightCommand("update")
public class UpdateCommand extends Command {
    @Override
    public Command exec() {
        if(getCache().get(getKey()) != null) {
            SimpleCache.getInstance().update(getKey(), getValue());
            setKeyExisted(true);
        } else {
            setKeyExisted(false);
        }
        return this;
    }

    @Override
    public String wrap() {
        if(!isKeyExisted()) {
            return "[Invalid Key] Key named '" + getKey() + "' is not existed !";
        }
        return "[OK] Done";
    }
}
