package zbl.moonlight.client;

import lombok.Getter;
import lombok.ToString;
import zbl.moonlight.client.exception.InvalidCommandException;
import zbl.moonlight.client.exception.InvalidMethodException;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.ResponseCode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MoonlightClient {
    private final String host;
    private final int port;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private Scanner scanner;

    public MoonlightClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void run() throws IOException {
        init();
        while (true) {
            try {
                print();
                Command command = readLine();
                send(command);
                showResponse();
            } catch (InvalidMethodException e) {
                printError("Method", e.getMessage());
                continue;
            } catch (InvalidCommandException e) {
                printError("Command", e.getMessage());
                continue;
            }
        }
    }

    public static void main(String[] args) throws IOException {
        MoonlightClient client = new MoonlightClient("127.0.0.1", 7820);
        client.run();
    }

    private void init() throws IOException {
        Socket socket = new Socket(host, port);
        inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        scanner = new Scanner(System.in);
    }

    private Command readLine() throws InvalidMethodException, InvalidCommandException {
        return new Command(scanner.nextLine());
    }

    private void print() {
        System.out.print("Moonlight > ");
    }

    private void printError(String type, String message) {
        System.out.println("[Invalid " + type + "][" + message + "]");
    }

    private void send(Command command) throws IOException {
        ByteBuffer key = ByteBuffer.wrap(command.getKey().toString().getBytes(StandardCharsets.UTF_8));
        ByteBuffer value = ByteBuffer.wrap(command.getValue().toString().getBytes(StandardCharsets.UTF_8));
        outputStream.write(MdtpRequest.encode(command.getCode(), key, value).array());
        outputStream.flush();
    }

    private void showResponse() throws IOException {
        byte responseCode = inputStream.readByte();
        int valueLength = inputStream.readInt();
        String responseValue = "";
        if(valueLength != 0) {
            byte[] responseValueBytes = new byte[valueLength];
            inputStream.read(responseValueBytes);
            responseValue += new String(responseValueBytes);
        }
        System.out.println("[" + ResponseCode.getCodeName(responseCode) + "][" + valueLength + "][" + responseValue + "]");
    }

    @ToString
    private class Command {
        private String command;
        private StringBuffer method;
        @Getter
        private StringBuffer key;
        @Getter
        private StringBuffer value;
        @Getter
        private byte code = (byte) 0xff;


        Command(String command) throws InvalidMethodException, InvalidCommandException {
            this.command = command;
            method = new StringBuffer();
            key = new StringBuffer();
            value = new StringBuffer();
            parse();
        }

        private void parse() throws InvalidMethodException, InvalidCommandException {
            StringBuffer current = method;
            for(int i = 0; i < command.length(); i ++) {
                if(command.charAt(i) == ' ') {
                    if(method.length() > 0) {
                        current = key;
                    }
                    if(key.length() > 0) {
                        current = value;
                    }
                    if(value.length() != 0) {
                        current.append(command.charAt(i));
                    }
                    continue;
                }

                current.append(command.charAt(i));
            }

            if(method.length() == 0) {
                throw new InvalidMethodException("method name is empty");
            }

            switch (method.toString()) {
                case "get":
                    code = MdtpMethod.GET;
                    verifyGet();
                    break;
                case "set":
                    code = MdtpMethod.SET;
                    verifySet();
                    break;
                case "delete":
                    code = MdtpMethod.DELETE;
                    verifyDelete();
                    break;
                case "exit":
                    code = MdtpMethod.EXIT;
                    break;
                case "system":
                    code = MdtpMethod.SYSTEM;
                    verifySystem();
                    break;
                default:
                    throw new InvalidMethodException("method \"" + method + "\" is invalid");
            }
        }

        private void verifyGet() throws InvalidCommandException {
            if(key.length() == 0) {
                throw new InvalidCommandException("key is empty");
            }
            value = new StringBuffer();
        }

        private void verifySet() throws InvalidCommandException {
            if(key.length() == 0) {
                throw new InvalidCommandException("key is empty");
            }
            if(value.length() == 0) {
                throw new InvalidCommandException("value is empty");
            }
        }

        private void verifyDelete() throws InvalidCommandException {
            if(key.length() == 0) {
                throw new InvalidCommandException("key is empty");
            }
            value = new StringBuffer();
        }

        private void verifySystem() throws InvalidCommandException {
            if(key.length() == 0) {
                throw new InvalidCommandException("key is empty");
            }
            if(value.length() == 0) {
                throw new InvalidCommandException("value is empty");
            }
        }
    }
}
