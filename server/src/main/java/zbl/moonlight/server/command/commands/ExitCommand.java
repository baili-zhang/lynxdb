package zbl.moonlight.server.command.commands;

import zbl.moonlight.server.command.Command;

public class ExitCommand extends Command {
    public ExitCommand() {
        super(null, null);
    }

    @Override
    public Command exec() {
        return this;
    }

    @Override
    public String wrap() {
        return "[Close Connection]";
    }
}
