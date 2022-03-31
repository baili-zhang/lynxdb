package zbl.moonlight.server.log;

import lombok.Getter;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.core.executor.Executor;

public class BinaryLogWriter extends Executor {
    @Getter
    private final String NAME = "BinaryLogWriter";
    /* 二进制日志文件 */
    private final BinaryLog binaryLog;
    private final Configuration config;

    /* 事件总线 */
    private final EventBus eventBus;

    public BinaryLogWriter(BinaryLog binaryLog) {
        MdtpServerContext context = MdtpServerContext.getInstance();
        eventBus = context.getEventBus();
        config = context.getConfiguration();
        this.binaryLog = binaryLog;
    }

    @Override
    public void run() {
        /*  */
    }
}
