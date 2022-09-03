package com.bailizhang.lynxdb.core.file;

import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class LogFileGroup {
    private static final int DEFAULT_FILE_SIZE = 4 * 1024 * 1024;
    private static final String SUFFIX = ".log";

    private final String groupDir;

    private int begin;
    private int end;

    public LogFileGroup(String dir) {
        groupDir = dir;
        File file = new File(dir);

        if(!file.isDirectory()) {
            throw new RuntimeException(dir + " is not a directory");
        }

        String[] filenames = file.list();

        if(filenames == null) {
            begin = 0;
            end = 0;
            return;
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

    public void append(BytesListConvertible convertible) {
         LogFile file = getFile(end);
         int maxIndex;

         try {
             maxIndex = file.append(convertible);
         } catch (IOException e) {
             throw new RuntimeException(e);
         }

         try {
             if(file.length() > DEFAULT_FILE_SIZE) {
                 end ++;
                 LogFile logFile = getFile(end);
                 logFile.createFile(maxIndex);
             }
         } catch (IOException e) {
             throw new RuntimeException(e);
         }
    }

    public void deleteRedundantFile(int i) {
        int p = begin;

        while(p <= end) {
            LogFile file = getFile(p);

            try {
                int end = file.end();
                if(i < end) {
                    break;
                }

                file.delete();
                begin ++;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public LogFile getFile(int i) {
        String filename = i + SUFFIX;

        try {
            return new LogFile(groupDir, filename);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
