package zbl.moonlight.server.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.command.annotation.MoonlightCommand;
import zbl.moonlight.server.command.concrete.*;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

public class CommandFactory {
    private static final String packageName = "zbl.moonlight.server.command.concrete";
    private static final ConcurrentHashMap<String, Command> commands = new ConcurrentHashMap<>();
    private static final Logger logger = LogManager.getLogger("CommandFactory");

    static {
        logger.info("scan commend defined in package \"{}\".", packageName);

        URL url = Thread.currentThread()
                .getContextClassLoader()
                .getResource(packageName.replace('.', '/'));

        String filePath = URLDecoder.decode(url.getFile(), StandardCharsets.UTF_8);
        File dir = new File(filePath);
        File[] files = dir.listFiles((file) -> file.getName().endsWith(".class"));

        String[] classNames = Arrays.stream(files).map((file) -> {
            String fileName = file.getName();
            return packageName + "." + fileName.substring(0, fileName.length() - 6);
        }).toArray(String[]::new);

        for(String className : classNames) {
            try {
                Class<Command> clazz = (Class<Command>) Class.forName(className);
                if(clazz.getAnnotation(MoonlightCommand.class) != null) {
                    String commandName = clazz.getAnnotation(MoonlightCommand.class).value();
                    Command command = clazz.getDeclaredConstructor().newInstance();
                    commands.put(commandName, command);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Command getCommand(String commandLine) throws CloneNotSupportedException {
        String[] commandArray = commandLine.trim().split("\\s+");
        Command command = commands.get(commandArray[0]).clone();
        if(commandArray.length > 1) {
            command.setKey(commandArray[1]);
        }
        if(commandArray.length > 2) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
            byteBuffer.put(commandArray[2].getBytes(StandardCharsets.UTF_8));
            command.setValue(byteBuffer);
        }
        return command;
    }
}
