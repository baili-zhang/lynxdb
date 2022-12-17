package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbEntity;
import com.bailizhang.lynxdb.core.common.G;

public class LynxDbCommand implements LynxDbEntity {
    private final LynxDbClient client;
    private final String[] commands;

    public LynxDbCommand(LynxDbClient client, String line) {
        this.client = client;
        commands = line.trim().split("\\s+");
    }

    public String name() {
        return commands.length < 1 ? null : commands[0].toLowerCase();
    }

    @Override
    public byte[] key() {
        return commands.length < 2 ? null : G.I.toBytes(commands[1]);
    }

    @Override
    public byte[] columnFamily() {
        return commands.length < 3 ? null : G.I.toBytes(commands[2]);
    }

    @Override
    public byte[] column() {
        return commands.length < 4 ? null : G.I.toBytes(commands[3]);
    }

    @Override
    public byte[] value() {
        return commands.length < 5 ? null : G.I.toBytes(commands[4]);
    }

    public int length() {
        return commands.length;
    }

    @Override
    public LynxDbClient client() {
        return client;
    }
}
