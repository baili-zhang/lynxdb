package zbl.moonlight.test;

import zbl.moonlight.client.common.Command;
import zbl.moonlight.client.exception.InvalidCommandException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Client {
    public static void send(Command command, DataOutputStream outputStream, int identifier) throws IOException, InvalidCommandException {
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
        outputStream.writeInt(identifier);
        outputStream.write(key);
        outputStream.write(value);
        outputStream.flush();
    }

    public static byte get(DataInputStream inputStream) throws IOException {
        byte responseCode = inputStream.readByte();
        int valueLength = inputStream.readInt();
        int identifier = inputStream.readInt();
        String responseValue = "";
        if(valueLength != 0) {
            byte[] responseValueBytes = new byte[valueLength];
            inputStream.read(responseValueBytes);
            responseValue += new String(responseValueBytes);
        }
        return responseCode;
    }
}
