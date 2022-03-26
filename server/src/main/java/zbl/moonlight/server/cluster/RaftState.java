package zbl.moonlight.server.cluster;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RaftState {
    private volatile RaftRole raftRole = RaftRole.Follower;

    /* -------------- 持久化数据 -------------- */
    /* 任期号 */
    private int currentTerm;
    /* 投票给了哪个节点 */
    private RaftNode votedFor;
    /* 需要同步的日志 */
    private String[] logs;

    /* ---------- 易失的状态（所有服务器） ------- */
    private int commitIndex;
    private int lastApplied;

    /* ----------- 易失的状态（Leader） -------- */
    private int[] nextIndex;
    private int[] matchIndex;
}
