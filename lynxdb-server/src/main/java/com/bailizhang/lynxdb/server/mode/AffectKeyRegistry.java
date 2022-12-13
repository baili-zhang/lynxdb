package com.bailizhang.lynxdb.server.mode;

import com.bailizhang.lynxdb.server.engine.affect.AffectKey;

import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class AffectKeyRegistry {
    private final ConcurrentHashMap<SelectionKey, HashSet<AffectKey>> selectionKeyMap
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<AffectKey, HashSet<SelectionKey>> affectKeyMap
            = new ConcurrentHashMap<>();

    public AffectKeyRegistry() {

    }

    public void register(SelectionKey selectionKey, AffectKey affectKey) {
        HashSet<AffectKey> affectKeys = selectionKeyMap.computeIfAbsent(
                selectionKey,
                k -> new HashSet<>()
        );

        affectKeys.add(affectKey);

        HashSet<SelectionKey> selectionKeys = affectKeyMap.computeIfAbsent(
                affectKey,
                k -> new HashSet<>()
        );

        selectionKeys.add(selectionKey);
    }

    public void deregister(SelectionKey selectionKey, AffectKey affectKey) {
        HashSet<AffectKey> affectKeys = selectionKeyMap.get(selectionKey);
        if(affectKeys != null) {
            affectKeys.remove(affectKey);
        }

        HashSet<SelectionKey> selectionKeys = affectKeyMap.get(affectKey);
        if(selectionKeys != null) {
            selectionKeys.remove(selectionKey);
        }
    }

    public void deregister(SelectionKey selectionKey) {
        HashSet<AffectKey> affectKeys = selectionKeyMap.remove(selectionKey);
        if(affectKeys == null || affectKeys.isEmpty()) {
            return;
        }

        for(AffectKey affectKey : affectKeys) {
            HashSet<SelectionKey> selectionKeys = affectKeyMap.get(affectKey);
            if(selectionKeys != null) {
                selectionKeys.remove(selectionKey);
            }
        }
    }

    public List<SelectionKey> selectionKeys(AffectKey affectKey) {
        HashSet<SelectionKey> selectionKeys = affectKeyMap.get(affectKey);

        List<SelectionKey> invalid = new ArrayList<>();
        List<SelectionKey> valid = new ArrayList<>();

        // 如果 selectionKey 无效的话，清空注册信息
        for(SelectionKey selectionKey : selectionKeys) {
            if(selectionKey.isValid()) {
                valid.add(selectionKey);
            } else {
                invalid.add(selectionKey);
            }
        }

        for(SelectionKey selectionKey : invalid) {
            deregister(selectionKey);
        }

        return valid;
    }
}
