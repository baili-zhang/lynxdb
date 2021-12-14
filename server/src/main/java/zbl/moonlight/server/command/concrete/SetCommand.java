package zbl.moonlight.server.command.concrete;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.annotation.MoonlightCommand;
import zbl.moonlight.server.engine.Cacheable;
import zbl.moonlight.server.engine.simple.SimpleCache;

import java.nio.ByteBuffer;

@MoonlightCommand("set")
public class SetCommand extends Command {
    @Override
    public Command exec() {
        if(getCache().get(getKey()) == null) {
            getCache().set(getKey(), getValue());
            setKeyExisted(false);
        } else {
            setKeyExisted(true);
        }
        return this;
    }

    @Override
    public String wrap() {
        if(isKeyExisted()) {
            return "[Invalid Key] Key named '" + getKey() + "' has already existed !";
        }
        return "[OK] Done";
    }
}
