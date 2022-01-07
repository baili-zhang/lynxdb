package zbl.moonlight.server.log;

import lombok.Getter;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.MdtpRequest;

public class BinaryLogWriter extends Executor<Event<?>> {
    @Getter
    private final String NAME = "BinaryLogWriter";
    /* 二进制日志文件 */
    private final BinaryLog binaryLog;

    public BinaryLogWriter(BinaryLog binaryLog, EventBus eventBus) {
        super(eventBus);
        this.binaryLog = binaryLog;
    }

    @Override
    public void run() {
        while (true) {
            Event<?> event = pollSleep();
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
