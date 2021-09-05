package rcache.engine;

import java.io.DataOutputStream;
import java.io.IOException;

public class Executor {
    private static StringHashTable stringHashTable = new StringHashTable();

    public static boolean execute(String[] commandArray, DataOutputStream outputStream) throws IOException {
        String command = null, key = null, value = null;

        if(commandArray.length > 0) {
            command = commandArray[0];
        }

        if(commandArray.length > 1) {
            key = commandArray[1];
        }

        if(commandArray.length > 2) {
            value = commandArray[2];
        }


        if(command != null && command.equals("exit")) {
            outputStream.writeUTF("close");
            return false;
        }

        if(command != null && command.equals("set") && key != null && value != null) {
            try {
                stringHashTable.set(key, value);
            } catch (Exception exception) {
                outputStream.writeUTF("key '" + key + "' is already existed !");
                return true;
            }
            outputStream.writeUTF("[set] " + key + ": " + value);
        }

        if(command != null && command.equals("get") && key != null) {
            String result;
            try {
                result = stringHashTable.get(key);
            } catch (Exception exception) {
                outputStream.writeUTF("key '" + key + "' is not existed !");
                return true;
            }

            outputStream.writeUTF(result);
        }

        if(command != null && command.equals("update") && key != null && value != null) {
            try {
                stringHashTable.update(key, value);
            } catch (Exception exception) {
                outputStream.writeUTF("key '" + key + "' is not existed !");
                return true;
            }
            outputStream.writeUTF("[update] " + key + ": " + value);
        }

        if(command != null && command.equals("delete") && key != null) {
            try {
                stringHashTable.delete(key);
            } catch (Exception exception) {
                outputStream.writeUTF("key '" + key + "' is not existed !");
                return true;
            }
            outputStream.writeUTF("[delete] " + key);
        }

        return true;
    }
}
