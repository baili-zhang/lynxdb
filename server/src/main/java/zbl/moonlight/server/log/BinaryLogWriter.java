package zbl.moonlight.server.log;

import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.MdtpRequest;

public class BinaryLogWriter extends Executor<Event<?>> {
    /* 二进制日志文件 */
    private final BinaryLog binaryLog;

    public BinaryLogWriter(BinaryLog binaryLog, EventBus eventBus, Thread eventBusThread) {
        super(eventBus, eventBusThread);
        this.binaryLog = binaryLog;
    }

    @Override
    public void run() {
        while (true) {
            Event<?> event = pollInSleep();
            if(event == null) {
                continue;
            }
            MdtpRequest request = (MdtpRequest) event.getValue();
            synchronized (request) {
                binaryLog.write(request);
            }
            event.setType(EventType.CLIENT_RESPONSE);
            send(event);
        }
    }
}
