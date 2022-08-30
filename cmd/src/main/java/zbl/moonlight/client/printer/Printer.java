package zbl.moonlight.client.printer;


import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.result.Result;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

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

    static void printError(String message) {
        System.out.println("Error: " + message);
    }

    static void printConnected(SocketAddress address) {
        String info = String.format("INFO: Has connected to [%s]", address);
        System.out.println(info);
    }

    static void printValueNotExist() {
        System.out.println("null");
    }

    static void printValue(byte[] value) {
        System.out.println(new String(value));
    }

    static void printOK() {
        System.out.println("OK");
    }

    static void printNotConnectServer() {
        System.out.println("INFO: Use \"connect [host] [port]\" to connect server firstly");
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
            case Result.SUCCESS -> Printer.printOK();
            case Result.SUCCESS_SHOW_COLUMN -> handleShowColumn(buffer);
            case Result.SUCCESS_SHOW_TABLE -> handleShowTable(buffer);
            case Result.Error.INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                Printer.printError(message);
            }

            default -> Printer.printError("Unknown Response Status Code");
        }
    }

    private static void handleShowTable(ByteBuffer buffer) {
        int columnSize = buffer.getInt();
        List<List<String>> table = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            List<String> row = new ArrayList<>();
            for(int i = 0; i < columnSize; i ++) {
                row.add(BufferUtils.getString(buffer));
            }
            table.add(row);
        }

        Printer.printTable(table);
    }

    private static void handleShowColumn(ByteBuffer buffer) {
        List<String> total = BufferUtils.toStringList(buffer);
        List<List<String>> table = total.stream().map(List::of).toList();
        Printer.printTable(table);
    }
}
