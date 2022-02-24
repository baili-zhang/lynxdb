# MDTP 协议（Moonlight Data Transfer Protocol）

## 请求

1 byte = 8 bit

|方法|键的长度|值的长度|标识号码|键的内容|值的内容|
|---|---|---|---|---|---|
|1 byte|1 byte|4 byte|4 byte|最大 255 byte|最大 2^31 - 1 byte|

### 方法
|方法值|方法名|说明|
|---|---|---|
|0x01|set|若key不存在，新增key，否则更新key|
|0x02|get|获取key|
|0x03|update|若key存在，更新key，否则报错（暂时不实现）|
|0x04|delete|删除key|
|0x05|exit|退出客户端|
|0x06|system|设置系统变量，例如：`system binlog off`（暂时未实现）|
|0x07|cluster|查看集群相关的信息（暂时未实现）|
|0x08|ping|用于心跳检测|

## 响应

|状态码|值的长度|标识号码|值的内容|
|---|---|---|---|
|1 byte|4 byte|4 byte|最大 2^31 - 1 byte|

### 状态码

|状态码|说明|
|---|---|
|0x01|值存在|
|0x02|值不存在|
|0x03|请求成功，没有值返回（如：delete）|
|0x04|请求出错|

