package com.bailizhang.lynxdb.cmd.printer;

import com.bailizhang.lynxdb.client.connection.LynxDbConnection;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.Pair;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public interface Printer {
    static void printPrompt(LynxDbConnection connection) {
        if(connection == null) {
            System.out.print("Moonlight> ");
        } else {
            String prompt = String.format("[%s] Moonlight> ", connection);
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

    static void printDisconnect(SelectionKey current) {
        String address = null;
        try {
            address = ((SocketChannel) current.channel()).getRemoteAddress().toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String message = String.format("INFO: Disconnect from [%s]", address);
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
