package rcache.response;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Response {
    private String value;
    private boolean isError;
    private String message;

    public String format() {
        return value + "\n" + (isError ? "ERROR" : "SUCCESS") + " " + message;
    }
}
