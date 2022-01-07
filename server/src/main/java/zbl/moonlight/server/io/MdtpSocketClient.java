package zbl.moonlight.server.io;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.executor.Executor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * 实现MDTP协议的客户端
 */
public class MdtpSocketClient extends Executor<Event<?>> {
    private final static Logger logger = LogManager.getLogger("MdtpSocketClient");
    @Getter
    private final String NAME = "MdtpSocketClient";
    private final String host;
    private final int port;

    private Selector selector;

    public MdtpSocketClient(String host, int port, EventBus eventBus) {
        super(eventBus);
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            SocketChannel channel = SocketChannel.open(new InetSocketAddress(host, port));
            channel.configureBlocking(false);
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            /* 不使用多线程，多个线程同时读写一个通道会出问题 */
            while (true) {
                if(!isEmpty() && channel.validOps() != SelectionKey.OP_CONNECT) {
                    channel.register(selector, channel.validOps() | SelectionKey.OP_WRITE);
                }

                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    if(selectionKey.isConnectable()) {
                        doConnect(selectionKey);
                    } else if(selectionKey.isReadable()) {
                        doRead(selectionKey);
                    } else if(selectionKey.isWritable()) {
                        doWrite(selectionKey);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void doConnect(SelectionKey selectionKey) throws IOException{
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        while (!channel.finishConnect()) {
            logger.info("client connecting......");
        }
        channel.register(selector, SelectionKey.OP_WRITE);
    }

    /* 将读取的响应发送给事件总线 */
    private void doRead(SelectionKey selectionKey) {
        System.out.println("client do read.");
    }

    private void doWrite(SelectionKey selectionKey) {
        Event<?> event = poll();
        if(event == null) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
            return;
        }
        /* 如果写完成 */
        if(true) {
            selectionKey.interestOpsOr(SelectionKey.OP_READ);
        }
    }
}
