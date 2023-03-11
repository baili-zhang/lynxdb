package com.bailizhang.lynxdb.core.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface FileUtils {
    String LOG_SUFFIX = ".log";

    static void delete(Path path) {
        File file = path.toFile();
        boolean success = false;

        if(file.isFile()) {
            success = file.delete();
        } else if(file.isDirectory()) {
            for(String sub : Objects.requireNonNull(file.list())) {
                Path subPath = Path.of(path.toString(), sub);
                delete(subPath);
            }

            success = file.delete();
        }

        if(!success) {
            throw new RuntimeException("Can not delete " + path);
        }
    }

    static void deleteSubs(Path path) {
        File file = path.toFile();

        if(file.isDirectory()) {
            for(String sub : Objects.requireNonNull(file.list())) {
                Path subPath = Path.of(path.toString(), sub);
                delete(subPath);
            }
        }
    }

    static void createDir(File file) {
        try {
            Files.createDirectories(file.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void createDirIfNotExisted(File file) {
        if(!file.exists()) {
            createDir(file);
        }
    }

    static void createDirIfNotExisted(String dir) {
        File file = new File(dir);
        createDirIfNotExisted(file);
    }

    static File createDirIfNotExisted(String dir, String... subDirs) {
        Path path = Path.of(dir, subDirs);
        File file = path.toFile();
        createDirIfNotExisted(file);
        return file;
    }

    static void createFile(File file) {
        try {
            Files.createFile(file.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void createFileIfNotExisted(File file) {
        if(!file.exists()) {
            createFile(file);
        }
    }

    static void createFileIfNotExisted(String filePath) {
        File file = new File(filePath);
        createFileIfNotExisted(file);
    }

    static File createFileIfNotExisted(String dir, String filename) {
        File file = new File(dir, filename);
        createFileIfNotExisted(file);
        return file;
    }

    static List<String> findSubDirs(String dir) {
        List<String> subDirs = new ArrayList<>();
        File file = new File(dir);

        String[] subs = file.list();
        if(subs == null) {
            return subDirs;
        }

        for(String sub : subs) {
            File subFile = new File(file, sub);
            if(subFile.isDirectory()) {
                subDirs.add(sub);
            }
        }

        return subDirs;
    }

    static List<String> findSubFiles(String dir) {
        return findSubFiles(new File(dir));
    }

    static List<String> findSubFiles(Path dirPath) {
        return findSubFiles(dirPath.toString());
    }

    static List<String> findSubFiles(File dirFile) {
        List<String> subFiles = new ArrayList<>();

        String[] subs = dirFile.list();
        if(subs == null) {
            return subFiles;
        }

        for(String sub : subs) {
            File subFile = new File(dirFile, sub);
            if(subFile.isFile()) {
                subFiles.add(sub);
            }
        }

        return subFiles;
    }
}
