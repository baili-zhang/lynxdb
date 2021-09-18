package moonlight.command.commands;

import moonlight.command.Command;

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
