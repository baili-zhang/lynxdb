package com.bailizhang.lynxdb.server.mode;


import com.bailizhang.lynxdb.ldtp.message.MessageKey;

import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class AffectKeyRegistry {
    private final ConcurrentHashMap<SelectionKey, HashSet<MessageKey>> selectionKeyMap
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MessageKey, HashSet<SelectionKey>> affectKeyMap
            = new ConcurrentHashMap<>();

    public AffectKeyRegistry() {

    }

    public void register(SelectionKey selectionKey, MessageKey messageKey) {
        HashSet<MessageKey> messageKeys = selectionKeyMap.computeIfAbsent(
                selectionKey,
                k -> new HashSet<>()
        );

        messageKeys.add(messageKey);

        HashSet<SelectionKey> selectionKeys = affectKeyMap.computeIfAbsent(
                messageKey,
                k -> new HashSet<>()
        );

        selectionKeys.add(selectionKey);
    }

    public void deregister(SelectionKey selectionKey, MessageKey messageKey) {
        HashSet<MessageKey> messageKeys = selectionKeyMap.get(selectionKey);
        if(messageKeys != null) {
            messageKeys.remove(messageKey);
        }

        HashSet<SelectionKey> selectionKeys = affectKeyMap.get(messageKey);
        if(selectionKeys != null) {
            selectionKeys.remove(selectionKey);
        }
    }

    public void deregister(SelectionKey selectionKey) {
        HashSet<MessageKey> messageKeys = selectionKeyMap.remove(selectionKey);
        if(messageKeys == null || messageKeys.isEmpty()) {
            return;
        }

        for(MessageKey messageKey : messageKeys) {
            HashSet<SelectionKey> selectionKeys = affectKeyMap.get(messageKey);
            if(selectionKeys != null) {
                selectionKeys.remove(selectionKey);
            }
        }
    }

    public List<SelectionKey> selectionKeys(MessageKey messageKey) {
        HashSet<SelectionKey> selectionKeys = affectKeyMap.get(messageKey);

        if(selectionKeys == null) {
            return new ArrayList<>();
        }

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
