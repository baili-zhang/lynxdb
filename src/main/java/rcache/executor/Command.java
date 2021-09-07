package rcache.executor;

import lombok.Data;

@Data
public class Command {
    private String lineCommand;

    private String command;
    private String key;
    private String value;

    public Command(String lineCommand) {
        this.lineCommand = lineCommand.trim();
    }

    public void resolve() throws Exception {
        String[] commandArray = lineCommand.split("\\s+");

        if(commandArray.length == 0) {
            throw new Exception();
        }

        if(commandArray.length > 0) {
            command = commandArray[0];
        }

        if(commandArray.length > 1) {
            key = commandArray[1];
        }

        if(commandArray.length > 2) {
            value = commandArray[2];
        }
    }
}
