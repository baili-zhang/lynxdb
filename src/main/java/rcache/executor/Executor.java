package rcache.executor;

import rcache.command.Command;
import rcache.engine.Cacheable;

public class Executor {
    private Cacheable<String, String> cache;

    public Executor(Cacheable cache) {
        this.cache = cache;
    }

    public ResultSet execute(Command command) {
        return command.exec(cache);
    }
}
