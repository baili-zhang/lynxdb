package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;

public class LynxDbCmdClient extends Shutdown {
    private static final String CONNECT = "connect";
    private static final String DISCONNECT = "disconnect";

    private static final String FIND = "find";
    private static final String INSERT = "insert";
    private static final String DELETE = "delete";
    private static final String REGISTER = "register";
    private static final String DEREGISTER = "deregister";
    private static final String EXIT = "exit";

    private static final String ERROR_COMMAND = "Invalid Command";

    private final LynxDbClient client = new LynxDbClient();
    private final Scanner scanner = new Scanner(System.in);

    private LynxDbConnection current;

    public LynxDbCmdClient() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    public void start() {
        client.start();

        while (isNotShutdown()) {
            Printer.printPrompt(current);

            String line = scanner.nextLine();
            LynxDbCommand command = new LynxDbCommand(line);

            switch (command.name()) {
                case CONNECT -> {
                    String address = G.I.toString(command.key());
                    ServerNode node = ServerNode.from(address);
                    client.connect(node);
                    current = client.connection(node);
                }

                case DISCONNECT -> {
                    current.disconnect();
                    current = null;
                }

                case FIND -> {
                    if(current == null) {
                        Printer.printNotConnectServer();
                    }

                    if(command.length() == 3) {
                        List<DbValue> dbValues = current.find(
                                command.key(),
                                command.columnFamily()
                        );

                        Printer.printDbValues(dbValues);
                    } else if(command.length() == 4) {
                        byte[] value = current.find(
                                command.key(),
                                command.columnFamily(),
                                command.column()
                        );

                        Printer.printRawMessage(G.I.toString(value));
                    } else {
                        Printer.printError(ERROR_COMMAND);
                    }
                }

                case INSERT -> {
                    if(current == null) {
                        Printer.printNotConnectServer();
                    }

                    if(command.length() != 5) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    current.insert(
                            command.key(),
                            command.columnFamily(),
                            command.column(),
                            command.value()
                    );
                }

                case DELETE -> {
                    if(current == null) {
                        Printer.printNotConnectServer();
                    }

                    if(command.length() != 4) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    current.delete(
                            command.key(),
                            command.columnFamily(),
                            command.column()
                    );
                }

                case REGISTER -> {
                    if(current == null) {
                        Printer.printNotConnectServer();
                    }

                    if(command.length() != 3) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    byte[] key = command.key();
                    byte[] columnFamily = command.columnFamily();

                    MessageKey messageKey = new MessageKey(key, columnFamily);
                    AffectHandler handler = new AffectHandler();
                    client.registerAffectHandler(messageKey, handler);

                    current.register(key, columnFamily);
                }

                case DEREGISTER -> {
                    if(current == null) {
                        Printer.printNotConnectServer();
                    }

                    if(command.length() != 3) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    current.deregister(
                            command.key(),
                            command.columnFamily()
                    );
                }

                case EXIT -> {
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
