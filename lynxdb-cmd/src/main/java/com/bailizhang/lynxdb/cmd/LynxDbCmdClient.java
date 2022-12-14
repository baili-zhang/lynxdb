package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class LynxDbCmdClient extends Shutdown {
    private static final String FIND = "find";
    private static final String INSERT = "insert";
    private static final String DELETE = "delete";
    private static final String REGISTER = "register";
    private static final String WATCH = "watch";

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
            Printer.printPrompt(client.current());

            String line = scanner.nextLine();
            String[] command = line.trim().split("\\s+");

            if(command.length < 1) {
                Printer.printError(ERROR_COMMAND);
                break;
            }

            switch (command[0]) {
                case FIND -> {
                    if(command.length == 3) {
                        List<DbValue> dbValues = client.find(
                                G.I.toBytes(command[1]),
                                G.I.toBytes(command[2])
                        );
                        printDbValues(dbValues);
                    } else if(command.length == 4) {
                        byte[] value = client.find(
                                G.I.toBytes(command[1]),
                                G.I.toBytes(command[2]),
                                G.I.toBytes(command[3])
                        );

                        Printer.printRawMessage(G.I.toString(value));
                    } else {
                        Printer.printError(ERROR_COMMAND);
                    }
                }

                case INSERT -> {
                    if(command.length != 5) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    client.insert(
                            G.I.toBytes(command[1]),
                            G.I.toBytes(command[2]),
                            G.I.toBytes(command[3]),
                            G.I.toBytes(command[4])
                    );
                }

                case DELETE -> {
                    if(command.length != 4) {
                        Printer.printError(ERROR_COMMAND);
                        break;
                    }

                    client.delete(
                            G.I.toBytes(command[1]),
                            G.I.toBytes(command[2]),
                            G.I.toBytes(command[3])
                    );
                }

                case REGISTER -> {
                    if(command.length == 3) {
                        client.register(
                                G.I.toBytes(command[1]),
                                G.I.toBytes(command[2])
                        );
                    } else {
                        Printer.printError(ERROR_COMMAND);
                    }
                }

                case WATCH -> {
                    AffectValue affectValue = client.onMessage();
                    printAffectValue(affectValue);
                }

                default -> Printer.printError(ERROR_COMMAND);
            }

        }
    }

    public static void main(String[] args) {
        LynxDbCmdClient client = new LynxDbCmdClient();
        client.start();
    }

    private void printDbValues(List<DbValue> dbValues) {
        List<List<String>> table = new ArrayList<>();
        List<String> header = List.of("Column", "Value");
        table.add(header);
        dbValues.forEach(dbValue -> {
            List<String> row = List.of(
                    G.I.toString(dbValue.column()),
                    G.I.toString(dbValue.value())
            );
            table.add(row);
        });

        Printer.printTable(table);
    }

    private void printAffectValue(AffectValue affectValue) {
        AffectKey affectKey = affectValue.affectKey();
        List<DbValue> dbValues = affectValue.dbValues();

        String template = "Affect key: %s, columnFamily: %s";
        String message = String.format(
                template,
                G.I.toString(affectKey.key()),
                G.I.toString(affectKey.columnFamily())
        );

        Printer.printRawMessage(message);
        printDbValues(dbValues);
    }
}
