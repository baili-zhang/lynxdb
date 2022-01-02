package zbl.moonlight.server.eventbus.subscriber;

import zbl.moonlight.server.eventbus.Subscriber;
import zbl.moonlight.server.protocol.MdtpRequest;

public class BinaryLogSubscriber implements Subscriber {
    @Override
    public void handle(MdtpRequest request) {
        System.out.println("write into binary log." + System.getProperty("user.dir"));
    }
}
