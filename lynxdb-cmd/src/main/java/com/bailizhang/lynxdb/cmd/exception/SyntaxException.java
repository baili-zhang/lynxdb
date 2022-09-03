package com.bailizhang.lynxdb.cmd.exception;

public class SyntaxException extends RuntimeException {
    private static final String TEMPLATE = "SyntaxError in: \"%s\"";
    public static final int MIN_LENGTH = 50;

    public SyntaxException(char[] chs, int curr) {
        super(buildMessage(chs, curr));
    }

    private static String buildMessage(char[] chs, int curr) {
        int remaining = chs.length - curr - 1;
        int length = Math.min(remaining, MIN_LENGTH);

        char[] dst = new char[length];
        System.arraycopy(chs, curr, dst, 0, length);
        return String.format(TEMPLATE, new String(dst));
    }
}
