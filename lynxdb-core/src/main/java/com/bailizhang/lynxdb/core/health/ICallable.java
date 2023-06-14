package com.bailizhang.lynxdb.core.health;

@FunctionalInterface
public interface ICallable<T> {
    T call();
}
