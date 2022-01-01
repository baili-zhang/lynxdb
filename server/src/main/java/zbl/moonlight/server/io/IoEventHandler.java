package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

public class IoEventHandler implements Runnable {
    private final Logger logger = LogManager.getLogger("IoEventHandler");
    private final SelectionKey selectionKey;
    private final CountDownLatch latch;
    private final Selector selector;
    private final Engine engine;

    public IoEventHandler (SelectionKey selectionKey, CountDownLatch latch, Selector selector, Engine engine) {
        this.selectionKey = selectionKey;
        this.latch = latch;
        this.selector = selector;
        this.engine = engine;
    }

    private void doAccept(SelectionKey selectionKey)
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ, new MdtpRequest());
        }
    }

    private void doRead(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        MdtpRequest mdtpRequest = (MdtpRequest) selectionKey.attachment();
        mdtpRequest.read(socketChannel);

        if(mdtpRequest.isReadCompleted()) {
            DynamicByteBuffer value = mdtpRequest.getValue();
            if(value != null) {
                value.flip();
            }
            MdtpResponse response = engine.exec(mdtpRequest);
            selectionKey.attach(response);
            selectionKey.interestOps(SelectionKey.OP_WRITE);
            logger.info("received command, " + mdtpRequest + ".");
        }
    }

    private void doWrite(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        MdtpResponse mdtpResponse = (MdtpResponse) selectionKey.attachment();
        mdtpResponse.write(socketChannel);

        if(mdtpResponse.isWriteCompleted()) {
            if(mdtpResponse.getValue() != null) {
                mdtpResponse.getValue().rewind();
            }
            selectionKey.attach(new MdtpRequest());
            selectionKey.interestOps(SelectionKey.OP_READ);
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
        } finally {
            latch.countDown();
        }
    }
}
