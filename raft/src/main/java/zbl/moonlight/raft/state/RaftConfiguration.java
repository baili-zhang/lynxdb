package zbl.moonlight.raft.state;

import zbl.moonlight.socket.client.ServerNode;

public interface RaftConfiguration {
    /**
     * 需要得到 leader 确认才能启动选举计时器
     */
    byte FOLLOWER = (byte) 0x01;
    /**
     * 需要连接一半以上的节点后才能转换成 candidate
     */
    byte CANDIDATE = (byte) 0x02;
    /**
     * 选举计时器超时即可转换成 candidate
     */
    byte LEADER = (byte) 0x03;

    byte leaderNode();

    ServerNode currentNode();
}
