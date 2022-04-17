# Raft协议相关

## Key格式

| hostname(or ip address) | separator | port   |
|-------------------------|-----------|--------|
| "host"                  | ":"       | "port" |

例如："127.0.0.1:7802"

## 请求投票Value格式

| TERM    | LAST_LOG_INDEX | LAST_LOG_TERM |
|---------|----------------|---------------|
| 4 byte  | 4 byte         | 4 byte        |

## 追加日志Value格式

| TERM   | PREV_LOG_INDEX | PREV_LOG_TERM | ENTRIES                    | LEADER_COMMIT |
|--------|----------------|---------------|----------------------------|---------------|
| 4 byte | 4 byte         | 4 byte        | (4byte) + (ENTRIES length) | 4 byte        |

ENTRIES格式：

| ENTRY size | ENTRY item                | ENTRY item                | ... |
|------------|---------------------------|---------------------------|-----|
| 4 byte     | (4 byte) + (ENTRY length) | (4 byte) + (ENTRY length) | ... |

ENTRY格式：

| TERM   | COMMIT_INDEX | METHOD | KEY                     | VALUE                     |
|--------|--------------|--------|-------------------------|---------------------------|
| 4 byte | 4 byte       | 1 byte | (4 byte) + (KEY length) | (4 byte) + (VALUE length) |



