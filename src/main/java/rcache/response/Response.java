package rcache.response;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Response {
    private String command;
    private String key;
    private String value;
    private boolean isError;
    private String message;

    public String format() {
        return "[" + command + "] '" + key + "' " + (isError ? "ERROR" : "SUCCESS") + " " + message + "\n" + value;
    }
}
