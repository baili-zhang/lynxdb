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

package com.bailizhang.lynxdb.core.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public interface FileUtils {
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

    static boolean exist(Path filePath) {
        File file = filePath.toFile();
        return file.exists();
    }

    static boolean notExist(Path filePath) {
        return !exist(filePath);
    }

    static void createFile(Path filePath) {
        try {
            Files.createFile(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    static File createFileIfNotExisted(String dir, String filename) {
        File baseDir = new File(dir);

        if(!baseDir.exists()) {
            createDir(baseDir);
        }

        File file = new File(dir, filename);
        if(!file.exists()) {
            try {
                Files.createFile(file.toPath());
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
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
