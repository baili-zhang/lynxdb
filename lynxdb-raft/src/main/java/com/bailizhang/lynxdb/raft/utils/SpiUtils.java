package com.bailizhang.lynxdb.raft.utils;

import java.util.Optional;
import java.util.ServiceLoader;

public interface SpiUtils {
    static  <T> T serviceLoad(Class<T> clazz) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(clazz);
        Optional<T> optional = serviceLoader.findFirst();

        if(optional.isEmpty()) {
            throw new RuntimeException("Can not find " + clazz.getSimpleName());
        }

        return optional.get();
    }
}
