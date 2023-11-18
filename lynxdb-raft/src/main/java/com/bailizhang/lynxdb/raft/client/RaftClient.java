package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.socket.client.SocketClient;

public class RaftClient extends SocketClient {
    private static final RaftClient client = new RaftClient();

    public RaftClient() {
    }

    public static RaftClient client() {
        return client;
    }

    @Override
    protected void doBeforeExecute() {
        setHandler(new RaftClientHandler());
        super.doBeforeExecute();
    }
}
