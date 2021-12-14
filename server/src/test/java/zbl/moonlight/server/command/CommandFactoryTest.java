package zbl.moonlight.server.command;

import org.junit.jupiter.api.Test;
import zbl.moonlight.server.command.annotation.MoonlightCommand;
import zbl.moonlight.server.command.concrete.ExitCommand;
import zbl.moonlight.server.command.concrete.UpdateCommand;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLDecoder;

import static org.junit.jupiter.api.Assertions.*;

class CommandFactoryTest {
    @Test
    void contextLoads() {
        new CommandFactory();
        new CommandFactory();
    }

    @Test
    void getCommand() throws Exception {
        CommandFactory commandFactory = new CommandFactory();
        Command command = commandFactory.getCommand("exit");
        assert command instanceof ExitCommand;
        Command command1 = commandFactory.getCommand("exit");
        assert command != command1;
    }
}