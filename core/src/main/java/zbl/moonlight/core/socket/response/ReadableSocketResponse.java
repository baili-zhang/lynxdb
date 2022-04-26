package zbl.moonlight.core.socket.response;

import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.Readable;

import java.io.IOException;

public class ReadableSocketResponse implements Readable {

    public ReadableSocketResponse(ServerNode node) {

    }

    @Override
    public void read() throws IOException {

    }

    @Override
    public boolean isReadCompleted() {
        return false;
    }
}
