package zbl.moonlight.server.eventbus;

public enum EventType {
    CLIENT_REQUEST,
    CLIENT_RESPONSE,
    BINARY_LOG_REQUEST,
    CLUSTER_REQUEST,
    CLUSTER_RESPONSE,
    LOG_RECOVER,
    NONE
}