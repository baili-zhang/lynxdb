package rcache.executor;

import rcache.engine.Cacheable;
import rcache.engine.simple.SimpleEngine;
import rcache.response.Response;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Executor {
    private Cacheable<String, String> cacheEngine;

    public Executor(Cacheable cacheEngine) {
        this.cacheEngine = cacheEngine;
    }

    public ResultSet<Object> execute(Command command) throws IOException {
        switch (command.getCommand()) {
            case "get":
                return doGet(command.getKey());
            case "set":
                return doSet(command.getKey(), command.getValue());
            case "update":
                return doUpdate(command.getKey(), command.getValue());
            case "delete":
                return doDelete(command.getKey());
            case "exit":
                return doExit();
        }

        return null;
    }

    public ResultSet<Object> doGet(String key) {
        ResultSet<Object> resultSet = new ResultSet<>();
        resultSet.setConnectionHold(true);

        String value = cacheEngine.get(key);
        Response response = new Response("get", key, value, false, "OK");

        resultSet.setResponse(response);

        return resultSet;
    }

    public ResultSet<Object> doSet(String key, String value) {
        ResultSet<Object> resultSet = new ResultSet<>();
        resultSet.setConnectionHold(true);
        cacheEngine.set(key, value);

        return resultSet;
    }

    public ResultSet<Object> doUpdate(String key, String value) {
        ResultSet<Object> resultSet = new ResultSet<>();
        resultSet.setConnectionHold(true);
        cacheEngine.update(key, value);

        return resultSet;
    }

    public ResultSet<Object> doDelete(String key) {
        ResultSet<Object> resultSet = new ResultSet<>();
        resultSet.setConnectionHold(true);
        cacheEngine.delete(key);

        return resultSet;
    }

    public ResultSet<Object> doExit() {
        ResultSet<Object> resultSet = new ResultSet<>();
        resultSet.setConnectionHold(false);

        return resultSet;
    }
}
