package com.bailizhang.lynxdb.core.recorder;

@FunctionalInterface
public interface ICallable<T> {
    T call();
}
