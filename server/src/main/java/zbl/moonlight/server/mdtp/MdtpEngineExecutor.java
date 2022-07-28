package zbl.moonlight.server.mdtp;

import zbl.moonlight.raft.server.RaftServer;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.socket.response.WritableSocketResponse;
import zbl.moonlight.storage.core.KvAdapter;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.kv.KvSingleGetQuery;

import static zbl.moonlight.server.annotations.MdtpMethod.*;

public class MdtpEngineExecutor extends EngineExecutor {
    public MdtpEngineExecutor(RaftServer server) {
        super(server, MdtpEngineExecutor.class);
    }

    @MdtpMethod(KV_SINGLE_GET)
    public WritableSocketResponse doSingleGetQuery(MdtpCommand command) {
        KvAdapter kvAdapter = kvDbMap.get(command.adapterName());
        byte[] value = kvAdapter.get(command.key());
        return new WritableSocketResponse(command.selectionKey(), command.serial(), value);
    }
}
