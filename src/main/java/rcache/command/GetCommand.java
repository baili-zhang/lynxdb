package rcache.command;

import rcache.engine.Cacheable;
import rcache.executor.ResultSet;
import rcache.response.Response;

public class GetCommand extends Command {

    public GetCommand(String key) {
        super(key, null);
    }

    @Override
    public ResultSet exec(Cacheable cache) {
        return new ResultSet(true, new Response((String)cache.get(key), false, "OK"));
    }
}
