package zbl.moonlight.client.common;

import lombok.Getter;
import lombok.ToString;
import zbl.moonlight.client.exception.InvalidCommandException;
import zbl.moonlight.client.exception.InvalidMethodException;
import zbl.moonlight.core.protocol.mdtp.MdtpMethod;

@ToString
public class Command {
    private String command;
    private StringBuffer method;
    @Getter
    private StringBuffer key;
    @Getter
    private StringBuffer value;
    @Getter
    private byte code = (byte) 0xff;

    public Command(String command) throws InvalidMethodException, InvalidCommandException {
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
            case "get" -> {
                code = MdtpMethod.GET;
                verifyGet();
            }
            case "set" -> {
                code = MdtpMethod.SET;
                verifySet();
            }
            case "delete" -> {
                code = MdtpMethod.DELETE;
                verifyDelete();
            }
            case "exit" -> {
                code = MdtpMethod.EXIT;
                verifyOnlyCommand();
            }
            case "system" -> {
                code = MdtpMethod.SYSTEM;
                verifySystem();
            }
            case "cluster" -> code = MdtpMethod.CLUSTER;
            case "ping" -> {
                code = MdtpMethod.PING;
                verifyOnlyCommand();
            }
            default -> throw new InvalidMethodException("method \"" + method + "\" is invalid");
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

    private void verifyOnlyCommand() {
        key = new StringBuffer();
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