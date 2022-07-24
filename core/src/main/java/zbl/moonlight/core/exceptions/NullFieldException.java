package zbl.moonlight.core.exceptions;

public class NullFieldException extends RuntimeException {
    private static final String FORMAT = "[%s] is null.";

    public NullFieldException(String name) {
        super(String.format(FORMAT, name));
    }
}
