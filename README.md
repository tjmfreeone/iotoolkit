#### 做了一点微小的工作，此工具可以使得数据搬运更方便🤭
#### 目前支持以下数据源的相互搬运:

- mongodb
- redis
- mysql
- elasticsearch

#### 这个工具支持一种读写规范，很容易就从短时间内编写出读写数据的代码，并且支持日志输出，包括读取进度，读取剩余时间，写入总数等等。例如

#### mysql:
```PYTHON
import asyncio
from iotoolkit.Packs import MySqlPack

async def foo():
    config = {
        ...
    }
    mysql_pack = MySqlPack(**config)
    getter = await mysql_pack.new_getter(table="fakers", batch_size=100)
    writer = await mysql_pack.new_writer(table="fakers2")
    async for lst in getter:
        for data in lst:
            ...
        await writer.write(lst)
```

#### 输出日志:
```
...
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlWriter | dst: fakers | write: 100 | written: 4500 | cost: 0.001s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 4600/7000, 65.71% | cost: 0.001s | left: 0.046s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlWriter | dst: fakers | write: 100 | written: 4600 | cost: 0.001s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 4700/7000, 67.14% | cost: 0.001s | left: 0.044s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlWriter | dst: fakers | write: 100 | written: 4700 | cost: 0.001s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 4800/7000, 68.57% | cost: 0.002s | left: 0.042s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlWriter | dst: fakers | write: 100 | written: 4800 | cost: 0.001s |
2023-03-05 00:28:55 pid:20023 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 4900/7000, 70.00% | cost: 0.001s | left: 0.039s |
...
```

#### 很容易就可以在不同类型的数据库中传输数据
```PYTHON
import asyncio
from iotoolkit.Packs import MySqlPack, MongoPack

async def foo():
    mysql_config = {
        ...
    }
    
    mongo_config = {
        ...
    }
    mysql_pack = MySqlPack(**mysql_config)
    mongo_pack = MongoPack(**mongo_config)
    getter = await mysql_pack.new_getter(table="fakers", batch_size=100)
    writer = await mongo_pack.new_writer("fakers")
    async for lst in getter:
        for data in lst:
            ...
        await writer.write(lst)
```

#### 输出日志:
```
...
2023-03-05 00:33:13 pid:20066 [INFO]: MongoWriter | dst: fakers | write: 100 | written: 3600 | cost: 0.001s |
2023-03-05 00:33:13 pid:20066 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 3700/14000, 26.43% | cost: 0.001s | left: 0.279s |
2023-03-05 00:33:13 pid:20066 [INFO]: MongoWriter | dst: fakers | write: 100 | written: 3700 | cost: 0.002s |
2023-03-05 00:33:13 pid:20066 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 3800/14000, 27.14% | cost: 0.003s | left: 0.275s |
2023-03-05 00:33:13 pid:20066 [INFO]: MongoWriter | dst: fakers | write: 100 | written: 3800 | cost: 0.002s |
2023-03-05 00:33:13 pid:20066 [INFO]: MySqlGetter | src:  | fetch: 100 | progress: 3900/14000, 27.86% | cost: 0.002s | left: 0.271s |
2023-03-05 00:33:13 pid:20066 [INFO]: MongoWriter | dst: fakers | write: 100 | written: 3900 | cost: 0.002s |
...
```

 