package com.bailizhang.lynxdb.core.utils;

import java.io.File;
import java.nio.file.Path;
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
}
