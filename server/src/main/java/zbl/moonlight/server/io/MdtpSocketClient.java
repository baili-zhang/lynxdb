package zbl.moonlight.server.io;

import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 实现MDTP协议的客户端
 */
public class MdtpSocketClient implements Runnable {
    private final ConcurrentLinkedQueue<MdtpRequest> requests;
    private final ConcurrentLinkedQueue<MdtpResponse> responses;
    private final String host;
    private final int port;

    public MdtpSocketClient(String host, int port, ConcurrentLinkedQueue<MdtpRequest> requests,
                            ConcurrentLinkedQueue<MdtpResponse> responses) {
        this.requests = requests;
        this.responses = responses;
        this.host = host;
        this.port = port;
    }

    public void offer(MdtpRequest request) {
        requests.offer(request);
    }

    @Override
    public void run() {
        try {
            SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
            socketChannel.configureBlocking(false);
            Selector selector = Selector.open();
            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();

                while (true) {

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
