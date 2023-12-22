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

package com.bailizhang.lynxdb.cmd.printer;

import java.util.ArrayList;
import java.util.List;

public class TablePrinter {
    public static final String CROSS = "+";
    public static final String UNDERLINE = "-";
    public static final String SPACE = " ";
    public static final String SEPARATOR = "|";

    public static final int MARGIN = 1;

    private final List<List<String>> table;
    private final List<Integer> width = new ArrayList<>();

    public TablePrinter(List<List<String>> data) {
        table = data;
    }

    public void print() {
        countWidth();
        printLine();

        for (int i = 0; i < table.size(); i++) {
            printRow(table.get(i));
            if(i == 0) printLine();
        }

        printLine();
    }

    private void countWidth() {
        for(String header : table.get(0)) {
            width.add(header.length());
        }

        for(List<String> row : table) {
            int i = 0;
            for(String cell : row) {
                cell = cell == null ? "null" : cell;
                width.set(i, Math.max(cell.length(), width.get(i)));
                i ++;
            }
        }

        width.replaceAll(integer -> integer + MARGIN * 2);
    }

    private void printLine() {
        StringBuilder line = new StringBuilder();
        line.append(CROSS);

        width.forEach(w -> {
            line.append(UNDERLINE.repeat(Math.max(0, w)));
            line.append(CROSS);
        });

        System.out.println(line);
    }

    private void printRow(List<String> row) {
        StringBuilder rowLine = new StringBuilder();
        rowLine.append(SEPARATOR);

        int i = 0;
        for (String cell : row) {
            cell = cell == null ? "null" : cell;

            int w = width.get(i ++);
            int spaceCount = w - cell.length();

            rowLine.append(SPACE.repeat(MARGIN));
            rowLine.append(cell);
            rowLine.append(SPACE.repeat(spaceCount - MARGIN));
            rowLine.append(SEPARATOR);
        }

        System.out.println(rowLine);
    }
}
