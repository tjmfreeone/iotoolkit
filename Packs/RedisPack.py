# @Author : taojinmin
# @Time : 2023/2/8 12:19
import aioredis
import redis
import json

from iotoolkit.Packs.Meta import BasePack, BaseGetter, BaseWriter
from iotoolkit.util import LogKit, DefaultValue, FuncSet
from types import FunctionType
from typing import List, Any
from time import time


class RedisPack(LogKit, BasePack):
    origin_conn_obj = {
        "pool": None,
        "cli": None
    }
    
    def __init__(self, *args, **kwargs):
        self.scheme = "redis"
        BasePack.__init__(self, *args, **kwargs)

    def is_ready(self):
        return all(list(self.origin_conn_obj.values()))

    async def _build_connect(self) -> None:
        """
        build connection for dbs
        """
        self.origin_conn_obj["pool"] = aioredis.ConnectionPool(decode_responses=True, **self.conn_config)
        self.origin_conn_obj["cli"] = aioredis.Redis(connection_pool=self.origin_conn_obj["pool"])

    @FuncSet.ensure_connected
    async def new_getter(self, key_name: str, key_type: str = "LIST", batch_size: int = 100, max_size: int = 0):
        if key_type.upper() == "LIST":
            return RedisListGetter(key_name=key_name, cli=self.origin_conn_obj["cli"], batch_size=batch_size, max_size=max_size)
        else:
            raise NotImplementedError("other key type getter is not implemented.")

    @FuncSet.ensure_connected
    async def new_writer(self, key_name: str, ):
        return RedisListWriter(key_name=key_name, cli=self.origin_conn_obj["cli"])


class RedisListGetter(BaseGetter):
    src_name: str = ""
    
    def __init__(self, key_name: str, cli: aioredis.Redis, batch_size: int = None, max_size: int = 0):
        super().__init__(batch_size=batch_size, max_size=max_size)
        self.key_name = key_name
        self.src_name = key_name
        self.cli = cli
        self.page = 0

    async def _get_total_count(self):
        if not self.total_cnt:
            self.total_cnt = await self.cli.llen(self.key_name)
            
    async def _get_next_lst(self) -> List:
        start = self.page * self.batch_size
        end = min(self.total_cnt, (self.page + 1) * self.batch_size - 1)
        next_lst = await self.cli.lrange(name=self.key_name, start=start, end=end)
        self.page += 1
        return next_lst
    
    
class RedisListWriter(BaseWriter):
    dst_name = ""
    
    def __init__(self, key_name: str, cli: aioredis.Redis):
        super().__init__()
        self.key_name = key_name
        self.dst_name = key_name
        self.cli = cli

    async def _handle_lst(self, lst: List[Any]):
        for each in lst:
            if isinstance(each, dict):
                each_push = json.dumps(each)
            else:
                each_push = str(each)
            await self.cli.lpush(self.key_name, each_push)
        
        
