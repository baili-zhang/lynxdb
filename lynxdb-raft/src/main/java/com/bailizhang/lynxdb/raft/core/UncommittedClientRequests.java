package com.bailizhang.lynxdb.raft.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class UncommittedClientRequests {
    private static final UncommittedClientRequests requests = new UncommittedClientRequests();

    private final PriorityQueue<ClientRequest> requestQueue =
            new PriorityQueue<>(Comparator.comparingInt(ClientRequest::idx));

    private UncommittedClientRequests() {

    }

    public static UncommittedClientRequests requests() {
        return requests;
    }

    public void add(ClientRequest request) {
        requestQueue.add(request);
    }

    public List<ClientRequest> pollUntil(int idx) {
        List<ClientRequest> polled = new ArrayList<>();

        while((requestQueue.peek() != null) && (requestQueue.peek().idx() <= idx)) {
             ClientRequest request = requestQueue.poll();
             polled.add(request);
        }

        return polled;
    }
}
