package zbl.moonlight.client;

import zbl.moonlight.client.common.Command;
import zbl.moonlight.client.exception.InvalidCommandException;
import zbl.moonlight.client.exception.InvalidMethodException;
import zbl.moonlight.server.protocol.mdtp.MdtpMethod;
import zbl.moonlight.server.protocol.mdtp.ResponseCode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MoonlightClient {
    private final String host;
    private final int port;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private Scanner scanner;
    private int identifier;
    private boolean connectionHold = true;

    public MoonlightClient(String host, int port) {
        this.host = host;
        this.port = port;
        identifier = 0;
    }

    public void runInTerminal() throws IOException {
        init();

        while (connectionHold) {
            try {
                print();
                Command command = readLine();
                send(command);
                if(command.getCode() == MdtpMethod.EXIT) {
                    connectionHold = false;
                } else {
                    showResponse();
                }
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
        client.runInTerminal();
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
        System.out.print("Moonlight> ");
    }

    private void printError(String type, String message) {
        System.out.println("[Invalid " + type + "][" + message + "]");
    }

    private void send(Command command) throws IOException, InvalidCommandException {
        byte method = command.getCode();
        byte[] key = command.getKey().toString().getBytes(StandardCharsets.UTF_8);
        byte[] value = command.getValue().toString().getBytes(StandardCharsets.UTF_8);
        if(key.length > 255) {
            throw new InvalidCommandException("key is too long.");
        }
        byte keyLength = (byte) key.length;
        int valueLength = value.length;

        /* 写方法和键的长度 */
        outputStream.write(new byte[]{method, keyLength});
        /* 写值的长度 */
        outputStream.writeInt(valueLength);
        outputStream.writeInt(++ identifier);
        outputStream.write(key);
        outputStream.write(value);
        outputStream.flush();
    }

    private void showResponse() throws IOException {
        byte responseCode = inputStream.readByte();
        int valueLength = inputStream.readInt();
        int identifier = inputStream.readInt();
        String responseValue = "";
        if(valueLength != 0) {
            byte[] responseValueBytes = new byte[valueLength];
            inputStream.read(responseValueBytes);
            responseValue += new String(responseValueBytes);
        }
        System.out.println("[" + ResponseCode.getCodeName(responseCode) + "]["
                + valueLength + "][" + identifier + "][" + responseValue + "]");
    }
}
