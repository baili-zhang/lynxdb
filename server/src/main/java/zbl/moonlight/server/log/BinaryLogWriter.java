package zbl.moonlight.server.log;

import lombok.Getter;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
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
    private final Configuration config;

    /* 事件总线 */
    private final EventBus eventBus;

    public BinaryLogWriter(BinaryLog binaryLog) {
        ServerContext context = ServerContext.getInstance();
        eventBus = context.getEventBus();
        config = context.getConfiguration();
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
            binaryLog.write(request);

            if(config.getSyncWriteLog()) {
                event.setType(EventType.CLIENT_REQUEST);
                eventBus.offer(event);
            }
        }
    }
}
