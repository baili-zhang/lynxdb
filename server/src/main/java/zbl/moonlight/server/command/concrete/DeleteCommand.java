package zbl.moonlight.server.command.concrete;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.annotation.MoonlightCommand;
import zbl.moonlight.server.engine.simple.SimpleCache;

@MoonlightCommand("delete")
public class DeleteCommand extends Command {
    @Override
    public Command exec() {
        if(getCache().get(getKey()) != null) {
            SimpleCache.getInstance().delete(getKey());
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
