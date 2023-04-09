package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.cmd.exception.ErrorFormatCommand;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Scanner;

public class LynxDbCmdClient extends Shutdown {
    private static final String CONNECT = "connect";
    private static final String DISCONNECT = "disconnect";

    private static final String FIND = "find";
    private static final String INSERT = "insert";
    private static final String DELETE = "delete";
    private static final String REGISTER = "register";
    private static final String DEREGISTER = "deregister";
    private static final String RANGE_NEXT = "range-next";
    private static final String EXIST = "exist";
    private static final String EXIT = "exit";
    private static final String JOIN = "join";

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

            LynxDbCommand command;

            try {
                command = new LynxDbCommand(line);
            } catch (ErrorFormatCommand ignored) {
                Printer.printError(ERROR_COMMAND);
                continue;
            }

            if(current == null && (!CONNECT.equals(command.name()) || EXIT.equals(command.name()))) {
                Printer.printNotConnectServer();
                continue;
            }

            try {
                handleCommand(command);
            } catch (ErrorFormatCommand ignored) {
                Printer.printError(ERROR_COMMAND);
            }
        }

        client.close();
    }

    public static void main(String[] args) {
        LynxDbCmdClient client = new LynxDbCmdClient();
        client.start();
    }

    private void handleCommand(LynxDbCommand command) throws ErrorFormatCommand {
        String name = command.name();

        switch (name) {
            case CONNECT    -> handleConnect(command);
            case DISCONNECT -> handleDisconnect(command);
            case FIND       -> handleFind(command);
            case INSERT     -> handleInsert(command);
            case DELETE     -> handleDelete(command);
            case REGISTER   -> handleRegister(command);
            case DEREGISTER -> handleDeregister(command);
            case RANGE_NEXT -> handleRangeNext(command);
            case EXIST      -> handleExist(command);
            case JOIN       -> handleJoin(command);
            case EXIT       -> handleExit(command);
            default         -> Printer.printError(ERROR_COMMAND);
        }
    }

    private void handleConnect(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(1);

        String address = command.poll();
        ServerNode node = ServerNode.from(address);
        client.connect(node);
        current = client.connection(node);
    }

    private void handleDisconnect(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(0);

        current.disconnect();
        current = null;
    }

    private void handleFind(LynxDbCommand command) throws ErrorFormatCommand {
        String key = command.poll();
        String columnFamily = command.poll();

        if(command.argsSize() != 2 && command.argsSize() != 3) {
            throw new ErrorFormatCommand();
        }

        try {
            String column = command.poll();
            byte[] value = current.find(
                    G.I.toBytes(key),
                    columnFamily,
                    column
            );
            Printer.printRawMessage(G.I.toString(value));
        } catch (ErrorFormatCommand ignored) {
            HashMap<String, byte[]> multiColumns = current.findMultiColumns(
                    G.I.toBytes(key),
                    columnFamily
            );
            Printer.printDbValues(multiColumns);
        }
    }

    private void handleInsert(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(4);

        String key = command.poll();
        String columnFamily = command.poll();
        String column = command.poll();
        String value = command.poll();

        current.insert(
                G.I.toBytes(key),
                columnFamily,
                column,
                G.I.toBytes(value)
        );
    }

    private void handleDelete(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(3);

        String key = command.poll();
        String columnFamily = command.poll();
        String column = command.poll();

        current.delete(
                G.I.toBytes(key),
                columnFamily,
                column
        );
    }

    private void handleRegister(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(2);

        String key = command.poll();
        String columnFamily = command.poll();

        byte[] keyBytes = G.I.toBytes(key);

        MessageKey messageKey = new MessageKey(keyBytes, columnFamily);
        AffectHandler handler = new AffectHandler();
        client.registerAffectHandler(messageKey, handler);

        current.register(keyBytes, columnFamily);
    }

    private void handleDeregister(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(2);

        String key = command.poll();
        String columnFamily = command.poll();

        current.deregister(
                G.I.toBytes(key),
                columnFamily
        );
    }

    private void handleRangeNext(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(4);

        String columnFamily = command.poll();
        String mainColumn = command.poll();
        String key = command.poll();
        int limit = command.pollInt();

        var multiKeys = current.rangeNext(
                columnFamily,
                mainColumn,
                G.I.toBytes(key),
                limit
        );

        Printer.printMultiKeys(multiKeys);
    }

    private void handleExist(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(3);

        String key = command.poll();
        String columnFamily = command.poll();
        String mainColumn = command.poll();

        boolean isExisted = current.existKey(
                G.I.toBytes(key),
                columnFamily,
                mainColumn
        );

        Printer.printBoolean(isExisted);
    }

    private void handleJoin(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(1);

        String node = command.poll();
        current.join(node);
    }

    private void handleExit(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(0);
        shutdown();
    }
}
