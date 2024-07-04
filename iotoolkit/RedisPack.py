# @Author : taojinmin
# @Time : 2023/2/8 12:19
import aioredis
import redis
import json

from iotoolkit.Meta import BasePack, BaseGetter, BaseWriter
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
    async def new_getter(self, key_name: str, key_type: str = "LIST", batch_size: int = 10, max_size: int = 0):
        if key_type.upper() == "LIST":
            return RedisListGetter(key_name=key_name, cli=self.origin_conn_obj["cli"], batch_size=batch_size, max_size=max_size)
        else:
            raise NotImplementedError("other key type getter is not implemented.")

    @FuncSet.ensure_connected
    async def new_writer(self, key_name: str, ):
        return RedisListWriter(key_name=key_name, cli=self.origin_conn_obj["cli"])


class RedisListGetter(BaseGetter):
    def __init__(self, key_name: str, cli: aioredis.Redis, batch_size: int = None, max_size: int = 0):
        self.key_name = key_name
        self.cli = cli

        self.batch_size = batch_size
        self.page = 0
        self.done_cnt = 0
        self.max_size = max_size
        self.total_cnt = 0

        self.finish_rate = 0
        self.first_fetch_ts = None
        self.last_fetch_ts = None

    def __aiter__(self):
        return self
    
    async def __anext__(self):
        if not self.total_cnt:
            self.total_cnt = await self.cli.llen(self.key_name)
        if not self.first_fetch_ts:
            self.first_fetch_ts = time()
            self.last_fetch_ts = self.first_fetch_ts
        
        start = self.page * self.batch_size
        end = min(self.total_cnt, (self.page + 1) * self.batch_size - 1)
        next_lst = await self.cli.lrange(name=self.key_name, start=start, end=end)
        self.page += 1

        curr_ts = time()
        cost_time = curr_ts - self.last_fetch_ts
        self.last_fetch_ts = curr_ts
        if not next_lst:
            msg = self.getter_finish_msg_tmpl.format(self.key_name, self.done_cnt,
                                                     FuncSet.x2humansTime(time() - self.first_fetch_ts))
            self.logger.info(msg)
            raise StopAsyncIteration
        # fetch stats
        self.done_cnt += len(next_lst)
        self.finish_rate = self.done_cnt / self.total_cnt
        finish_rate_str = "%.2f" % (self.finish_rate * 100)
        fetch_cnt_per_sec = self.done_cnt / (time() - self.first_fetch_ts)
        # fetch_cnt_per_sec = len(next_lst) / cost_time
        left_time = (self.total_cnt - self.done_cnt) / fetch_cnt_per_sec
        msg = self.getter_batch_msg_tmpl.format(self.key_name, len(next_lst), self.done_cnt, self.total_cnt,
                                                finish_rate_str,
                                                FuncSet.x2humansTime(cost_time), FuncSet.x2humansTime(left_time))
        self.logger.info(msg)
       
        return next_lst
    
    
class RedisListWriter(BaseWriter):
    def __init__(self, key_name: str, cli: aioredis.Redis):
        self.key_name = key_name
        self.cli = cli
        self.written = 0

    async def write(self, lst: List[Any]):
        try:
            before_write_ts = time()
            for each in lst:
                if isinstance(each, dict):
                    each_push = json.dumps(each)
                else:
                    each_push = str(each)
                await self.cli.lpush(self.key_name, each_push)
            cost_time = time() - before_write_ts
            self.written += len(lst)
            self.logger.info(self.writer_batch_msg_tmpl.format(self.key_name, len(lst), self.written,
                                                               FuncSet.x2humansTime(cost_time)))
        except Exception as e:
            self.logger.error(e)
        
