package zbl.moonlight.server.eventbus.subscriber;

import zbl.moonlight.server.eventbus.Subscriber;
import zbl.moonlight.server.protocol.MdtpRequest;

public class ClusterSubscriber implements Subscriber {
    @Override
    public void handle(MdtpRequest request) {
        System.out.println("send to slave node.");
    }
}
