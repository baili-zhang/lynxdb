package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

public class LynxDbCmdClient extends Shutdown {
    private static final String FIND = "find";
    private static final String INSERT = "insert";
    private static final String DELETE = "delete";
    private static final String REGISTER = "register";
    private static final String DEREGISTER = "deregister";
    private static final String EXIT = "exit";

    private static final String ERROR_COMMAND = "Invalid Command";

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 7820;
    private static final int MESSAGE_PORT = 7263;

    private final LynxDbClient client = new LynxDbClient();
    private final Scanner scanner = new Scanner(System.in);

    public LynxDbCmdClient() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    public void start() {
        client.start();
        client.connect(HOST, PORT);
        client.registerConnect(HOST, MESSAGE_PORT);

        while (isNotShutdown()) {
            Printer.printPrompt(client.selectionKey());

            String line = scanner.nextLine();
            LynxDbCommand command = new LynxDbCommand(client, line);

            switch (command.name()) {
                case FIND -> {
                    if(command.length() == 3) {
                        List<DbValue> dbValues = command.findByKey();
                        Printer.printDbValues(dbValues);
                    } else if(command.length() == 4) {
                        byte[] value = command.find();
                        Printer.printRawMessage(G.I.toString(value));
                    } else {
                        Printer.printError(ERROR_COMMAND);
                    }
                }

                case INSERT -> {
                    if(command.length() != 5) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }
                    command.insert();
                }

                case DELETE -> {
                    if(command.length() != 4) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }
                    command.delete();
                }

                case REGISTER -> {
                    if(command.length() != 3) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    byte[] key = command.key();
                    byte[] columnFamily = command.columnFamily();

                    MessageKey messageKey = new MessageKey(key, columnFamily);
                    AffectHandler handler = new AffectHandler(client);
                    client.registerAffectHandler(messageKey, handler);

                    command.register();
                }

                case DEREGISTER -> {
                    if(command.length() != 3) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }
                    command.deregister();
                }

                case EXIT -> {
                    command.exit();
                    shutdown();
                }

                default -> Printer.printError(ERROR_COMMAND);
            }
        }

        client.close();
    }

    public static void main(String[] args) {
        LynxDbCmdClient client = new LynxDbCmdClient();
        client.start();
    }
}
