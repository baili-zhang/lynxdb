package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.core.common.G;

/**
 * TODO: 重写这个类
 */
public class LynxDbCommand {
    private final String[] commands;

    public LynxDbCommand(String line) {
        commands = line.trim().split("\\s+");
    }

    public String name() {
        return commands.length < 1 ? null : commands[0].toLowerCase();
    }

    public byte[] key() {
        return commands.length < 2 ? null : G.I.toBytes(commands[1]);
    }

    public String columnFamily() {
        return commands.length < 3 ? null : commands[2];
    }

    public String column() {
        return commands.length < 4 ? null : commands[3];
    }

    public byte[] value() {
        return commands.length < 5 ? null : G.I.toBytes(commands[4]);
    }
    public String[] array() {
        return commands;
    }

    public int length() {
        return commands.length;
    }
}
