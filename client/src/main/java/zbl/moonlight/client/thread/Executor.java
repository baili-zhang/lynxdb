package zbl.moonlight.client.thread;

import zbl.moonlight.client.common.Command;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Executor implements Runnable {
    private static AtomicInteger count = new AtomicInteger(0);
    private final ConcurrentLinkedQueue<Command> commands;

    public Executor(ConcurrentLinkedQueue<Command> commands) {
        this.commands = commands;
    }

    @Override
    public void run() {
        while (true) {
            if(commands.size() > 0) {
                send(commands.poll());
            }
        }
    }

    private void send(Command command) {
        if(command == null) {
            return;
        }
        System.out.println(command + ", " + count.getAndIncrement());
    }
}
