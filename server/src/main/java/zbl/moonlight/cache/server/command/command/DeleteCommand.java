package zbl.moonlight.cache.server.command.command;

import zbl.moonlight.cache.server.command.Command;
import zbl.moonlight.cache.server.engine.simple.SimpleCache;

public class DeleteCommand extends Command {
    public DeleteCommand(String key) {
        super(key, null);
    }

    @Override
    public Command exec() {
        if(SimpleCache.getInstance().get(key) != null) {
            SimpleCache.getInstance().delete(key);
            isKeyExisted = true;
        } else {
            isKeyExisted = false;
        }
        return this;
    }

    @Override
    public String wrap() {
        if(!isKeyExisted) {
            return "[Invalid Key] Key named '" + key + "' is not existed !";
        }
        return "[OK] Done";
    }
}
