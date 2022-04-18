package zbl.moonlight.client;

import zbl.moonlight.client.common.Command;
import zbl.moonlight.client.exception.InvalidCommandException;
import zbl.moonlight.client.exception.InvalidMethodException;
import zbl.moonlight.core.protocol.Serializer;
import zbl.moonlight.core.protocol.nio.SocketState;
import zbl.moonlight.core.socket.SocketSchema;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequestSchema;
import zbl.moonlight.server.mdtp.ResponseStatus;

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
    private int serial;
    private boolean connectionHold = true;

    public MoonlightClient(String host, int port) {
        this.host = host;
        this.port = port;
        serial = 0;
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
            } catch (InvalidCommandException e) {
                printError("Command", e.getMessage());
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
        System.out.println("Error: (Invalid " + type + ") " + message + "");
    }

    /* TODO:取消硬编码，使用MdtpRequestSchema和MdtpResponseSchema重构 */
    private void send(Command command) throws IOException, InvalidCommandException {
        byte method = command.getCode();
        byte[] key = command.getKey().toString().getBytes(StandardCharsets.UTF_8);
        byte[] value = command.getValue().toString().getBytes(StandardCharsets.UTF_8);

        Serializer serializer = new Serializer(MdtpRequestSchema.class);
        serializer.mapPut(SocketSchema.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        serializer.mapPut(MdtpRequestSchema.METHOD, new byte[]{method});
        serializer.mapPut(MdtpRequestSchema.KEY, key);
        serializer.mapPut(MdtpRequestSchema.VALUE, value);
        serializer.mapPut(MdtpRequestSchema.SERIAL, ByteArrayUtils.fromInt(serial ++));

        outputStream.write(serializer.getByteBuffer().array());
        outputStream.flush();
    }

    private void showResponse() throws IOException {
        inputStream.readInt();
        byte socketState = inputStream.readByte();
        byte responseCode = inputStream.readByte();
        int identifier = inputStream.readInt();
        int valueLength = inputStream.readInt();
        String responseValue = "";
        if(valueLength != 0) {
            byte[] responseValueBytes = new byte[valueLength];
            inputStream.read(responseValueBytes);
            responseValue += new String(responseValueBytes);
        }

        System.out.println("------------------ RESPONSE ------------------");
        System.out.println("Status: " + ResponseStatus.getCodeName(responseCode)
                + "\nSerial number: " + identifier
                + "\nValue length: " + valueLength
                + "\nValue: " + responseValue);
        System.out.println("---------------- RESPONSE END ----------------");
    }
}
