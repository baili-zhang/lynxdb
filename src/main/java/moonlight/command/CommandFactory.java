package moonlight.command;

import moonlight.command.commands.*;

public class CommandFactory {
    public Command getCommand(String commandLine) {
        String[] commandArray = commandLine.trim().split("\\s+");

        if(commandArray.length > 2) {
            switch (commandArray[0]) {
                case "set":
                    return new SetCommand(commandArray[1], commandArray[2]);
                case "update":
                    return new UpdateCommand(commandArray[1], commandArray[2]);
            }
        }

        if(commandArray.length > 1) {
            switch (commandArray[0]) {
                case "get":
                    return new GetCommand(commandArray[1]);
                case "delete":
                    return new DeleteCommand(commandArray[1]);
            }
        }

        if(commandArray.length > 0) {
            switch (commandArray[0]) {
                case "exit":
                    return new ExitCommand();
            }
        }

        return null;
    }
}
