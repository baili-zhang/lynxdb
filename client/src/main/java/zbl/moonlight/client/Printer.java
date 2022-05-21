package zbl.moonlight.client;

import zbl.moonlight.core.socket.client.ServerNode;

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

    static void printConnected(ServerNode node) {
        String info = String.format("INFO: Has connected to [%s]", node);
        System.out.println(info);
    }
}
