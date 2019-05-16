# naiveDB
《数据库》大作业


## 元数据管理模块

#### 目录结构设计
```
databases
	- auth.json（存储数据库列表，不同数据库的权限信息，用户列表及密码）
   - database1
       - table1(暂时没定格式)
       - meta.json(存储此数据库的表的元数据信息)
   - database2
   		- meta.json 
   
```

#### 文件设计

* auth.json

```json
{
	"users_list":[
		"admin",
		"user1",
		"user2"
    ],
    "databases_list":[
        "database1",
        "database2"
    ],
    "password_map":{
        "admin":"123456",
        "user1":"123",
        "user2":"456"
    },
	"auth_map": {
		"database1":[
			"admin",
			"user1"
		],
		"database2":[
			"admin",
			"user2"
		]
	}
}
```
* 单一数据库的 ``meta.json``

```json
{
	"person":{
		"types":[7,3],
		"filepath":"databases/database1/person.db",
		"offsets":[512,4],
		"pktypes":[3],
		"notnull":[true,true],
		"pkattrs":["ID"],
		"attrs":["name","ID"]
	}
}
```

#### 功能说明

##### 已完成
* 支持新建，删除数据库（仅允许admin）
* 支持切换数据库（仅当此用户有该数据库的权限）
* 支持新建，删除表
* 支持用户登陆认证（admin为管理员，user1仅具有database1的权限，user2仅具有database2的权限）
* 支持查询所有数据库名（无权限限制）
* 支持查询单个数据库的所有表名（仅当用户具有该数据库权限）
* meta.json的持久化写回（可控制频率）

##### 待完成
* auth.json的持久化写回

#### 注意事项
* 注意在结束客户连接后记得显式调用meta.json和 auth.json写回方法

#### 参考
* [读取json文件](https://www.cnblogs.com/geek1116/p/7413619.html)
* [漂亮的JSON显示](https://www.json.cn)
* [commons-io api](http://commons.apache.org/proper/commons-io/javadocs/api-2.6/index.html)
* [org.json](https://www.cnblogs.com/geek1116/p/7413619.html)
* [json与java对象的转换](https://blog.csdn.net/qq_37918817/article/details/80740638)


