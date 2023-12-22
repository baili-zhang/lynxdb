/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.cmd.exception.ErrorFormatCommand;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.core.recorder.RecordOption;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

public class LynxDbCmdClient extends Shutdown {
    private static final String CONNECT = "connect";
    private static final String DISCONNECT = "disconnect";

    private static final String FIND = "find";
    private static final String INSERT = "insert";
    private static final String DELETE = "delete";
    private static final String RANGE_NEXT = "range-next";
    private static final String RANGE_BEFORE = "range-before";
    private static final String EXIST = "exist";
    private static final String EXIT = "exit";
    private static final String JOIN = "join";
    private static final String FLIGHT_RECORDER = "flight-recorder";

    private static final String ERROR_COMMAND = "Invalid Command";

    private final LynxDbClient client = new LynxDbClient();
    private final Scanner scanner = new Scanner(System.in);

    private LynxDbConnection connection;

    public LynxDbCmdClient() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    public void start() {
        client.start();

        while (isNotShutdown()) {
            Printer.printPrompt(connection);

            String line = scanner.nextLine();

            LynxDbCommand command;

            try {
                command = new LynxDbCommand(line);
            } catch (ErrorFormatCommand ignored) {
                Printer.printError(ERROR_COMMAND);
                continue;
            }

            if(connection == null && !CONNECT.equals(command.name()) && !EXIT.equals(command.name())) {
                Printer.printNotConnectServer();
                continue;
            }

            try {
                handleCommand(command);
            } catch (ErrorFormatCommand ignored) {
                Printer.printError(ERROR_COMMAND);
            } catch (ConnectException e) {
                Printer.printDisconnect(connection);
            }
        }

        client.close();
    }

    public static void main(String[] args) {
        LynxDbCmdClient client = new LynxDbCmdClient();
        client.start();
    }

    private void handleCommand(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        String name = command.name();

        switch (name) {
            case CONNECT            -> handleConnect(command);
            case DISCONNECT         -> handleDisconnect(command);
            case FIND               -> handleFind(command);
            case INSERT             -> handleInsert(command);
            case DELETE             -> handleDelete(command);
            case RANGE_NEXT         -> handleRangeNext(command);
            case RANGE_BEFORE       -> handleRangeBefore(command);
            case EXIST              -> handleExist(command);
            case JOIN               -> handleJoin(command);
            case EXIT               -> handleExit(command);
            case FLIGHT_RECORDER    -> handleFlightRecorder(command);
            default                 -> Printer.printError(ERROR_COMMAND);
        }
    }

    private void handleConnect(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSize(1);

        String address = command.poll();
        ServerNode node;

        try {
            node = ServerNode.from(address);
        } catch (RuntimeException e) {
            Printer.printNotConnectServer();
            return;
        }

        connection = client.createConnection(node);
        connection.connect();
    }

    private void handleDisconnect(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(0);

        connection.disconnect();
        connection = null;
    }

    private void handleFind(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        if(command.argsSize() != 2 && command.argsSize() != 3) {
            throw new ErrorFormatCommand();
        }

        String key = command.poll();
        String columnFamily = command.poll();

        try {
            String column = command.poll();
            byte[] value = connection.find(
                    G.I.toBytes(key),
                    columnFamily,
                    column
            );
            Printer.printRawMessage(G.I.toString(value));
        } catch (ErrorFormatCommand | ConnectException ignored) {
            HashMap<String, byte[]> multiColumns = connection.findMultiColumns(
                    G.I.toBytes(key),
                    columnFamily
            );
            Printer.printDbValues(multiColumns);
        }
    }

    private void handleInsert(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSize(4);

        String key = command.poll();
        String columnFamily = command.poll();
        String column = command.poll();
        String value = command.poll();

        connection.insert(
                G.I.toBytes(key),
                columnFamily,
                column,
                G.I.toBytes(value)
        );
    }

    private void handleDelete(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSize(3);

        String key = command.poll();
        String columnFamily = command.poll();
        String column = command.poll();

        connection.delete(
                G.I.toBytes(key),
                columnFamily,
                column
        );
    }

    private void handleRangeNext(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSizeMoreThan(4);

        String columnFamily = command.poll();
        String mainColumn = command.poll();
        String key = command.poll();
        int limit = command.pollInt();
        String[] findColumns = command.pollRemaining();

        var multiKeys = connection.rangeNext(
                columnFamily,
                mainColumn,
                G.I.toBytes(key),
                limit,
                findColumns
        );

        Printer.printMultiKeys(multiKeys);
    }

    private void handleRangeBefore(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSizeMoreThan(4);

        String columnFamily = command.poll();
        String mainColumn = command.poll();
        String key = command.poll();
        int limit = command.pollInt();
        String[] findColumns = command.pollRemaining();

        var multiKeys = connection.rangeBefore(
                columnFamily,
                mainColumn,
                G.I.toBytes(key),
                limit,
                findColumns
        );

        Printer.printMultiKeys(multiKeys);
    }

    private void handleExist(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSize(3);

        String key = command.poll();
        String columnFamily = command.poll();
        String mainColumn = command.poll();

        boolean isExisted = connection.existKey(
                G.I.toBytes(key),
                columnFamily,
                mainColumn
        );

        Printer.printBoolean(isExisted);
    }

    private void handleJoin(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSize(1);

        String node = command.poll();
        connection.join(node);
    }

    private void handleFlightRecorder(LynxDbCommand command) throws ErrorFormatCommand, ConnectException {
        command.checkArgsSize(0);
        List<Pair<RecordOption, Long>> data = connection.flightRecorder();

        List<List<String>> table = new ArrayList<>();

        List<String> header = List.of("Name", "Value");
        table.add(header);

        data.forEach(pair -> {
            List<String> row = new ArrayList<>();
            RecordOption option = pair.left();
            long val = pair.right();

            row.add(String.format("%s (%s)", option.name(), option.unit().unitName()));
            row.add(String.valueOf(val));

            table.add(row);
        });

        Printer.printTable(table);
    }

    private void handleExit(LynxDbCommand command) throws ErrorFormatCommand {
        command.checkArgsSize(0);
        shutdown();
    }
}
