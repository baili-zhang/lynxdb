package zbl.moonlight.client;


import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

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
}
