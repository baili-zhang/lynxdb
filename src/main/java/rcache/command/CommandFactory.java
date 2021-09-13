package rcache.command;

public class CommandFactory {
    public Command getCommand(String commandLine) {
        String[] commandArray = commandLine.trim().split("\\s+");

        if(commandArray.length > 2) {

        }

        if(commandArray.length > 1) {
            switch (commandArray[0]) {
                case "get":
                    return new GetCommand(commandArray[1]);
            }
        }

        if(commandArray.length > 0) {

        }

        return null;
    }
}
