# NaiveDB使用手册

刘译键 2016013239 

苏宇荣 2015080045



##使用方式 

**服务器启动：**

```shell
java -jar Server.jar [ip] [port]
```

ip 默认为 localhost

port 默认为 12306



**客户端启动：**

```bash
java -jar Client.jar [port]
```

port 默认为 12306



**客户端退出：**

```
bye
quit
logout
```

任意指令即可退出。




## import 方式

```shell
import path_to_file.sql
```



## 加分项目

- 存储模块
	- 自定义的高效文件格式
- 元数据管理模块
  - 可以创建或删除数据库实例，并在数据库实例中切换
  - 支持以数据库为单位的账号密码式权限认证
- 支持create/drop/use database语句
  - 支持show databases 和 show database <database_name>语句
- 查询模块
  - 主键支持多列
  - 支持任意多个and,or相连的复合on语句
  - 支持natural join语句
  - 支持任意多个and, or相连的复合where语句
- 客户端通信模块
  - 美观的表格式输出
  - 人性化的错误提示	
