package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.state.StateMachine;

import java.nio.channels.SelectionKey;

public class MdtpStateMachine implements StateMachine {
    @Override
    public void apply(Entry[] entries) {

    }

    @Override
    public void exec(SelectionKey key, byte[] command) {

    }
}
