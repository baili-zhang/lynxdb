package com.bailizhang.lynxdb.raft.core;

import java.util.List;
import java.util.PriorityQueue;

public class UncommittedClientRequests {
    private static final UncommittedClientRequests requests = new UncommittedClientRequests();

    private final PriorityQueue<ClientRequest> requestQueue =
            new PriorityQueue<>();

    private UncommittedClientRequests() {

    }

    public static UncommittedClientRequests requests() {
        return requests;
    }

    public void add(ClientRequest request) {
        requestQueue.add(request);
    }

    public List<ClientRequest> pollUntil(int idx) {
        throw new UnsupportedOperationException();
    }
}
