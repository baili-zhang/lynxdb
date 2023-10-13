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
