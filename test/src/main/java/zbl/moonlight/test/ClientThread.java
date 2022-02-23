package zbl.moonlight.test;

import zbl.moonlight.client.common.Command;
import zbl.moonlight.server.protocol.ResponseCode;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class ClientThread implements Runnable {

    private final String host;
    private final int port;
    private final CountDownLatch latch;
    private final int begin;
    private final int offset;
    private final AtomicLong successCount;

    public ClientThread(String host, int port, CountDownLatch latch,
                        int begin, int offset, AtomicLong successCount) {
        this.host = host;
        this.port = port;
        this.latch = latch;
        this.begin = begin;
        this.offset = offset;
        this.successCount = successCount;
    }

    @Override
    public void run() {
        try {
            int identifier = 0;
            Socket socket = new Socket(host, port);
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            String content = "java.util.concurrent.ConcurrentSkipListSet";
            for (int j = 0; j < 1; j++) {
                content += content;
            }

            for (int j = begin; j < begin + offset; j++) {
                String setCommand = "set " + j + " " + content + j;
                Command command = new Command(setCommand);
                Client.send(command, outputStream, ++ identifier);
            }

            for (int j = 0; j < offset; j++) {
                try {
                    byte code = Client.get(inputStream);
                    if(code == ResponseCode.SUCCESS_NO_VALUE) {
                        successCount.incrementAndGet();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
