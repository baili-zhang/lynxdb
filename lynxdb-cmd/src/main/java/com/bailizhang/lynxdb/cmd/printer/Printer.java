package com.bailizhang.lynxdb.cmd.printer;


import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static com.bailizhang.lynxdb.cmd.LynxDbCmdClient.KEY_HEADER;
import static com.bailizhang.lynxdb.server.annotations.LdtpCode.*;

public interface Printer {
    static void printPrompt(SelectionKey current) {
        if(current == null) {
            System.out.print("Moonlight> ");
        } else {
            try {
                String address = ((SocketChannel) current.channel()).getRemoteAddress().toString();
                String prompt = String.format("[%s] Moonlight> ", address);
                System.out.print(prompt);
            } catch (IOException e) {
                e.printStackTrace();
            }
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

    static void printResponse(byte[] response) {
        ByteBuffer buffer = ByteBuffer.wrap(response);
        byte code = buffer.get();

        switch (code) {
            case SUCCESS -> Printer.printOK();
            case SUCCESS_WITH_TABLE -> handleShowTable(buffer);
            case INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                Printer.printError(message);
            }

            default -> Printer.printError("Unknown Response Status Code");
        }
    }

    static void printKvPairs(byte[] response, List<String> header) {
        ByteBuffer buffer = ByteBuffer.wrap(response);
        byte code = buffer.get();

        switch (code) {
            case SUCCESS_WITH_KV_PAIRS -> {
                int columnSize = 2;
                List<List<String>> table = new ArrayList<>();
                table.add(header);

                while(!BufferUtils.isOver(buffer)) {
                    List<String> row = new ArrayList<>();
                    for(int i = 0; i < columnSize; i ++) {
                        row.add(BufferUtils.getString(buffer));
                    }
                    table.add(row);
                }

                Printer.printTable(table);
            }

            case INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                Printer.printError(message);
            }

            default -> Printer.printError("Unknown Response Status Code");
        }
    }

    static void handleShowTable(ByteBuffer buffer) {
        int columnSize = buffer.getInt();
        List<List<String>> table = new ArrayList<>();

        List<String> header = new ArrayList<>();
        header.add(KEY_HEADER);

        for(int i = 0; i < columnSize; i ++) {
            header.add(BufferUtils.getString(buffer));
        }

        table.add(header);

        while(!BufferUtils.isOver(buffer)) {
            List<String> row = new ArrayList<>();
            for(int i = 0; i < columnSize + 1; i ++) {
                row.add(BufferUtils.getString(buffer));
            }
            table.add(row);
        }

        Printer.printTable(table);
    }

    static void printList(byte[] response, String header) {
        ByteBuffer buffer = ByteBuffer.wrap(response);
        byte code = buffer.get();

        switch (code) {
            case SUCCESS_WITH_LIST -> {
                List<String> total = BufferUtils.toStringList(buffer);
                List<List<String>> table = new ArrayList<>();
                List<List<String>> body = total.stream().map(List::of).toList();

                table.add(List.of(header));
                table.addAll(body);
                Printer.printTable(table);
            }

            case INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                Printer.printError(message);
            }

            default -> Printer.printError("Unknown Response Status Code");
        }
    }
}
