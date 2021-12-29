package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class IoEventHandler implements Runnable {
    private final Logger logger = LogManager.getLogger("IoEventHandler");
    private final SelectionKey selectionKey;
    private Engine engine;

    public IoEventHandler (SelectionKey selectionKey, Engine engine) {
        this.selectionKey = selectionKey;
        this.engine = engine;
    }

    private void doAccept(SelectionKey selectionKey)
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);
        IoEvent ioEvent = (IoEvent) selectionKey.attachment();
        ioEvent.setSocketChannel(channel);
    }

    private void doRead(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        IoEvent ioEvent = (IoEvent) selectionKey.attachment();
        MdtpRequest mdtpRequest = ioEvent.getMdtpRequest();
        if(mdtpRequest == null) {
            mdtpRequest = new MdtpRequest();
            ioEvent.setMdtpRequest(mdtpRequest);
        }
        mdtpRequest.read(socketChannel);

        if(mdtpRequest.isReadFinished()) {
            ioEvent.setFinished(true);
            MdtpResponse response = engine.exec(mdtpRequest);
            ioEvent.setMdtpResponse(response);
            ioEvent.setMdtpRequest(null);
            logger.info("received command, " + mdtpRequest + ".");
        }
    }

    private void doWrite(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        IoEvent ioEvent = (IoEvent) selectionKey.attachment();
        MdtpResponse mdtpResponse = ioEvent.getMdtpResponse();
        mdtpResponse.write(socketChannel);

        if(mdtpResponse.isWriteCompleted()) {
            ioEvent.setFinished(true);
        }
    }

    @Override
    public void run() {
        try {
            if(selectionKey.isAcceptable()) {
                doAccept(selectionKey);
            } else if (selectionKey.isReadable()) {
                doRead(selectionKey);
            } else if (selectionKey.isWritable()) {
                doWrite(selectionKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
