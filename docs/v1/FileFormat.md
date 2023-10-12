# 文件格式

## LogGroup 文件

文件名：`index.lgm`


## LogRegion 文件

文件后缀：`.lgr` 

**文件格式**

| 名称      | 说明   |
|---------|------|
| Meta 区  | 元数据区 |
| index 区 | 索引区  |
| Data 区  | 数据区  |

**Meta 区**

| 字段                   | 长度      | 说明      |
|----------------------|---------|---------|
| 魔数（Magic Number）     | 4 bytes | int 类型  |
| 删除条目总数（Delete Count） | 4 bytes | int 类型  |
| 条目总数（Total）          | 4 bytes | int 类型  |
| 条目起始序号（Begin）        | 4 bytes | int 类型  |
| 条目结束序号（End）          | 4 bytes | int 类型  |
| CRC 校验和（CRC）         | 8 bytes | long 类型 |
