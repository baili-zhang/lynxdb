/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    public String[] pollRemaining() {
        if(args.size() > 0) {
            return args.toArray(String[]::new);
        }

        return new String[0];
    }

    public void checkArgsSize(int size) throws ErrorFormatCommand {
        if(args.size() != size) {
            throw new ErrorFormatCommand();
        }
    }

    public void checkArgsSizeMoreThan(int size) throws ErrorFormatCommand {
        if(args.size() < size) {
            throw new ErrorFormatCommand();
        }
    }

    public int argsSize() {
        return args.size();
    }
}
