package zbl.moonlight.server.eventbus.subscriber;

import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.ClusterNodeConfiguration;
import zbl.moonlight.server.eventbus.Subscriber;
import zbl.moonlight.server.io.MdtpSocketClient;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClusterSubscriber implements Subscriber {
    private final List<MdtpSocketClient> clients = new ArrayList<>();
    private final List<ConcurrentLinkedQueue<MdtpResponse>> responseList = new ArrayList<>();

    public ClusterSubscriber(ClusterConfiguration config) {
        for(ClusterNodeConfiguration nodeConfig : config.getNodes()) {
            /* 设置响应队列 */
            ConcurrentLinkedQueue<MdtpResponse> responses = new ConcurrentLinkedQueue<>();

            MdtpSocketClient client = new MdtpSocketClient(nodeConfig.getHost(),
                    nodeConfig.getPort(), new ConcurrentLinkedQueue<>(), responses);
            /* 将响应队列添加到响应队列列表 */
            responseList.add(responses);
            /* 将集群客户端添加到客户端列表 */
            clients.add(client);
        }
    }

    @Override
    public void handle(MdtpRequest request) {
        for (MdtpSocketClient client : clients) {
            client.offer(request);
        }
    }
}
