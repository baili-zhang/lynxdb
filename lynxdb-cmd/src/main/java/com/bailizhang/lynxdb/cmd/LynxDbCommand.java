package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.core.common.G;

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

    public byte[] columnFamily() {
        return commands.length < 3 ? null : G.I.toBytes(commands[2]);
    }

    public byte[] column() {
        return commands.length < 4 ? null : G.I.toBytes(commands[3]);
    }

    public byte[] value() {
        return commands.length < 5 ? null : G.I.toBytes(commands[4]);
    }

    public int length() {
        return commands.length;
    }
}
