package rcache.command;

import lombok.Data;
import rcache.engine.Cacheable;
import rcache.executor.ResultSet;

@Data
public abstract class Command {
    protected String key;
    protected String value;

    public Command(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public abstract ResultSet exec(Cacheable cache);
}
