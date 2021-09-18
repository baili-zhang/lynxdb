package moonlight.command.commands;

import moonlight.command.Command;
import moonlight.engine.simple.SimpleCache;

public class SetCommand extends Command {

    public SetCommand(String key, String value) {
        super(key, value);
    }

    @Override
    public Command exec() {
        if(SimpleCache.getInstance().get(key) == null) {
            SimpleCache.getInstance().set(key, value);
            isKeyExisted = false;
        } else {
            isKeyExisted = true;
        }
        return this;
    }

    @Override
    public String wrap() {
        if(isKeyExisted) {
            return "[Invalid Key] Key named '" + key + "' has already existed !";
        }
        return "[OK] Done";
    }
}
