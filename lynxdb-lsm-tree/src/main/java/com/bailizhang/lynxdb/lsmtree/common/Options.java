package com.bailizhang.lynxdb.lsmtree.common;

public class Options {
    private boolean createColumnFamilyIfNotExisted = false;

    public Options() {

    }

    public void createColumnFamilyIfNotExisted(boolean val) {
        createColumnFamilyIfNotExisted = val;
    }

    public boolean createColumnFamilyIfNotExisted() {
        return createColumnFamilyIfNotExisted;
    }
}
