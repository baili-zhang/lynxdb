# 文件格式

## LogGroup 文件

文件名：`index.lgm`

LogGroup 的序列号从 1 开始，逐次递增。


## LogRegion 文件

文件后缀：`.lgr` 

**文件格式**

| 名称      | 说明   |
|---------|------|
| Meta 区  | 元数据区 |
| index 区 | 索引区  |
| Data 区  | 数据区  |

**Meta 区**

| 字段                     | 长度      | 说明      |
|------------------------|---------|---------|
| 魔数（Magic Number）       | 4 bytes | int 类型  |
| 删除条目总长度（Delete Length） | 4 bytes | int 类型  |
| 条目总长度（Total Length）    | 4 bytes | int 类型  |
| 条目起始序号（Begin）          | 4 bytes | int 类型  |
| 条目结束序号（End）            | 4 bytes | int 类型  |
| CRC 校验和（CRC）           | 8 bytes | long 类型 |

**Index Entry**

| 字段                   | 长度      | 说明      |
|----------------------|---------|---------|
| 删除标志                 | 1 bytes |         |
| 数据条目开始位置（Data Begin） | 4 bytes | int 类型  |
| 数据条目长度（Data Length）  | 4 bytes | int 类型  |
| CRC 校验和（CRC）         | 8 bytes | long 类型 |

## SSTable 格式

文件后缀：`.sst`

**文件格式**

| 名称             | 说明    |
|----------------|-------|
| Meta 区         | 元数据   |
| Bloom Filter 区 | 布隆过滤器 |
| First Index 区  | 一级索引区 |
| Second Index 区 | 二级索引区 |
| Data 区         | 数据区   |

**Meta 区**

| 字段                         | 长度      | 说明        |
|----------------------------|---------|-----------|
| Meta Region Length         | 4 bytes | int 类型    |
| 魔数（Magic Number）           | 4 bytes | int 类型    |
| Bloom Filter Region Length | 4 bytes | int 类型    |
| First Index Region Length  | 4 bytes | int 类型    |
| Second Index Region Length | 4 bytes | int 类型    |
| Data Region Length         | 4 bytes | int 类型    |
| Max Key Size               | 4 bytes | int 类型    |
| Meta Header CRC 校验和        | 8 bytes | long 类型   |
| Begin Key Length           | 4 bytes | int 类型    |
| Begin Key                  | 若干      | byte[] 类型 |
| End Key Length             | 4 bytes | int 类型    |
| End Key                    | 若干      | byte[] 类型 |
| CRC 校验和（CRC）               | 8 bytes | long 类型   |

**Bloom Filter 区**

包含持久化的布隆过滤器。

**First Index 区**

First Index Entry 格式：

| 字段                 | 长度      | 说明        |
|--------------------|---------|-----------|
| Length             | 4 bytes | int 类型    |
| Begin Key Length   | 4 bytes | int 类型    |
| Begin Key          | 若干      | byte[] 类型 |
| Begin Key 二级索引的偏移量 | 4 bytes | int       |
| End Key Length     | 4 bytes | int 类型    |
| End Key            | 若干      | byte[] 类型 |
| End Key 二级索引的偏移量   | 4 bytes | int       |
| CRC 校验和（CRC）       | 8 bytes | long 类型   |

**Second Index 区**

Second Index Entry 格式：

| 字段                   | 长度      | 说明      |
|----------------------|---------|---------|
| 删除标志                 | 1 bytes |         |
| 数据条目开始位置（Data Begin） | 4 bytes | int 类型  |
| 数据条目长度（Data Length）  | 4 bytes | int 类型  |
| CRC 校验和（CRC）         | 8 bytes | long 类型 |

**Data 区**

Key Entry 格式：

| 字段                 | 长度      | 说明         |
|--------------------|---------|------------|
| 删除标志               | 1 bytes |            |
| Key Length         | 4 bytes | int 类型     |
| Key                | 若干      | bytes[] 类型 |
| Value Global Index | 4 bytes | int 类型     |
| Timeout            | 8 bytes | long 类型    |
| CRC 校验和（CRC）       | 8 bytes | long 类型    |
