package com.bailizhang.lynxdb.core.file;

import java.io.File;
import java.util.Arrays;

public class LogFileGroup {
    private final int begin;
    private final int end;

    public LogFileGroup(String dir) {
        File file = new File(dir);

        if(!file.isDirectory()) {
            throw new RuntimeException(dir + " is not a directory");
        }

        String[] filenames = file.list();

        if(filenames == null) {
            throw new RuntimeException();
        }

        Integer[] indexList = Arrays.stream(filenames)
                .map(filename -> {
                    String[] temp = filename.split("\\.");
                    return Integer.valueOf(temp[0]);
                }).toArray(Integer[]::new);

        Arrays.sort(indexList);

        begin = indexList[0];
        end = indexList[indexList.length - 1];
    }
}
