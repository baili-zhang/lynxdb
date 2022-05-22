package zbl.moonlight.client;

import zbl.moonlight.core.raft.response.BytesConvertable;

import java.nio.charset.StandardCharsets;

public record Command(
        String name,
        String key,
        String value
) implements BytesConvertable {
    /**
     * 退出客户端
     */
    public static final String EXIT_COMMEND = "exit";

    /**
     * 客户端请求：连接服务器
     */
    public static final String CONNECT_COMMAND = "connect";

    /**
     * 断开连接
     */
    public static final String DISCONNECT_COMMAND = "disconnect";

    /**
     * 服务端请求：获取 key 的 value
     */
    public static final String GET_COMMAND = "get";

    /**
     * 服务端请求：设置 key
     */
    public static final String SET_COMMAND = "set";

    /**
     * 服务端请求：删除 key
     */
    public static final String DELETE_COMMAND = "delete";

    /**
     * 集群请求：获取集群信息
     */
    public static final String CLUSTER_COMMAND = "cluster";

    public static Command fromString(String source) {
        StringBuilder commandName = new StringBuilder();
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();

        StringBuilder current = commandName;
        for(int i = 0; i < source.length(); i ++) {
            if(source.charAt(i) == ' ') {
                if(commandName.length() > 0) {
                    current = key;
                }
                if(key.length() > 0) {
                    current = value;
                }
                if(value.length() != 0) {
                    current.append(source.charAt(i));
                }
                continue;
            }

            current.append(source.charAt(i));
        }

        return new Command(commandName.toString().toLowerCase(),
                key.toString(), value.toString());
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
