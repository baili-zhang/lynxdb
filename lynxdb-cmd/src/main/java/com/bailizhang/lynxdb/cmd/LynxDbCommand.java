package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.cmd.exception.ErrorFormatCommand;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;


public class LynxDbCommand {
    private final String name;
    private final Queue<String> args = new LinkedList<>();

    public LynxDbCommand(String line) throws ErrorFormatCommand {
        String[] str = line.trim().split("\\s+");

        if(str.length == 0) {
            throw new ErrorFormatCommand();
        }

        name = str[0].toLowerCase();
        args.addAll(Arrays.asList(str).subList(1, str.length));
    }

    public String name() {
        return name;
    }

    public String poll() throws ErrorFormatCommand {
        String str = args.poll();

        if(str == null) {
            throw new ErrorFormatCommand();
        }

        return str;
    }

    public int pollInt() throws ErrorFormatCommand {
        String str = poll();

        try {
            return Integer.parseInt(str);
        } catch (NumberFormatException e) {
            throw new ErrorFormatCommand();
        }
    }

    public void checkArgsSize(int size) throws ErrorFormatCommand {
        if(args.size() != size) {
            throw new ErrorFormatCommand();
        }
    }

    public int argsSize() {
        return args.size();
    }
}
