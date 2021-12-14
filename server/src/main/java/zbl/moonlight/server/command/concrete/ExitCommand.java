package zbl.moonlight.server.command.concrete;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.annotation.MoonlightCommand;

@MoonlightCommand("exit")
public class ExitCommand extends Command {
    @Override
    public Command exec() {
        return this;
    }

    @Override
    public String wrap() {
        return "[Close Connection]";
    }
}
