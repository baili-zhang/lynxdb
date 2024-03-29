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

package com.bailizhang.lynxdb.cmd.printer;

import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Pair;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface Printer {
    static void printPrompt(LynxDbConnection connection) {
        if(connection == null) {
            System.out.print("LynxDB-cli> ");
        } else {
            String prompt = String.format("[%s] LynxDB-cli> ", connection);
            System.out.print(prompt);
        }
    }

    static void printRawMessage(String message) {
        System.out.println(message);
    }

    static void printError(String message) {
        System.out.println("Error: " + message);
    }

    static void printConnected(SocketAddress address) {
        String info = String.format("INFO: Has connected to [%s]", address);
        System.out.println(info);
    }

    static void printOK() {
        System.out.println("OK");
    }

    static void printNotConnectServer() {
        System.out.println("INFO: Use \"connect [host]:[port]\" to connect server firstly");
    }

    static void printDisconnect(LynxDbConnection connection) {
        String message = String.format("ERROR: Disconnect from [%s]", connection.serverNode());
        System.out.println(message);
    }

    static void printTable(List<List<String>> table) {
        new TablePrinter(table).print();
    }

    static void printDbValues(HashMap<String, byte[]> multiColumns) {
        List<List<String>> table = new ArrayList<>();
        List<String> header = List.of("Column", "Value");
        table.add(header);
        multiColumns.forEach((column, value) -> {
            List<String> row = new ArrayList<>();
            row.add(column);
            row.add(G.I.toString(value));
            table.add(row);
        });

        Printer.printTable(table);
    }

    static void printMultiKeys(List<Pair<byte[], HashMap<String, byte[]>>> multiKeys) {
        List<List<String>> table = new ArrayList<>();
        List<String> header = List.of("Key", "Column", "Value");
        table.add(header);

        multiKeys.forEach(pair -> {
            byte[] key = pair.left();
            pair.right().forEach((column, value) -> {
                List<String> row = new ArrayList<>();
                row.add(G.I.toString(key));
                row.add(column);
                row.add(G.I.toString(value));
                table.add(row);
            });
        });

        Printer.printTable(table);
    }

    static void printBoolean(boolean isExisted) {
        System.out.println(isExisted);
    }
}
