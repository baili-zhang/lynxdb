package zbl.moonlight.client;


import zbl.moonlight.socket.client.ServerNode;

import java.net.SocketAddress;

public interface Printer {
    static void printPrompt(ServerNode current) {
        if(current == null) {
            System.out.print("Moonlight> ");
        } else {
            String prompt = String.format("[%s] Moonlight> ", current);
            System.out.print(prompt);
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

    static void printDisconnect(ServerNode node) {
        String message = String.format("INFO: Disconnect from [%s]", node);
        System.out.println(message);
    }
}
