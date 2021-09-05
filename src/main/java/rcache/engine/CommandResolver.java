package rcache.engine;

public class CommandResolver {
    public static String[] line(String command) {
        command = command.trim();
        return command.split("\\s+");
    }
}
