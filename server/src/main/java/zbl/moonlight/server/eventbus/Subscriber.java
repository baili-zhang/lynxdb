package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.MdtpRequest;

@FunctionalInterface
/* 提供[事件名]和[入口队列列表] */
public interface Subscriber {
    void handle (MdtpRequest request);
}
