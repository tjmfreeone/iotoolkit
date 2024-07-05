# @Author : taojinmin
# @Time : 2023/2/6 18:32
import asyncio
from abc import ABC, abstractmethod, abstractproperty
from typing import List, Any
from types import MappingProxyType, FunctionType
from iotoolkit.PackManager import pack_manager
from urllib.parse import urlparse
from iotoolkit.util import LogKit, FuncSet
from time import time


class OriginConnObj(LogKit):
    """接收动态属性"""
    def __init__(self):
        super().__setattr__('attrs', dict())

    def __getattr__(self, key):
        if key not in self.attrs:
            return None
        return self.attrs[key]

    def __setattr__(self, key, value):
        self.attrs[key] = value


class BasePack(ABC):
    origin_conn_obj: OriginConnObj

    def __init__(self, uri: str = "", host: str = "", port: int = None, username: str = "", password: str = "",
                 db: str = "", *args, **kwargs):
        """
        :param uri: 统一资源定位符(es无uri)
        :param host: IP地址
        :param port: 端口
        :param username: 用户名
        :param password: 密码
        :param db: 指定db, 该字段在es中无效
        """
        if not uri and not any([
            host, port, username, password, db
        ]):
            raise ValueError("connection params error!")
        self.origin_conn_obj = OriginConnObj()
        # 优先解析uri
        if uri:
            self.uri_parse = urlparse(uri)
            self.conn_config = MappingProxyType(
                {
                    "host": uri_parse.hostname,
                    "port": uri_parse.port,
                    "username": uri_parse.username,
                    "password": uri_parse.password,
                    "db": uri_parse.path[1:],  # db only for mongodb/mysql/redis
                }
            )
            self.scheme = self.uri_parse.scheme
        else:
            self.conn_config = MappingProxyType(
                {
                    "host": host,
                    "port": int(port),
                    "username": username,
                    "password": password,
                    "db": db,  # db only for mongodb/mysql/redis
                }
            )
            query = "&".join([k + "=" + v for k, v in kwargs.items()])
            # es没有uri，但是统一规范，这里不对es做特殊处理
            uri = self.scheme + "://{username}:{password}@{host}:{port}/{db}".format(**self.conn_config) + "?" + query
            self.uri_parse = urlparse(uri)

        pack_manager.register_pack(self)

    @abstractmethod
    async def _build_connect(self) -> None:
        """
        build connection for dbs
        """
        ...

    @abstractmethod
    def is_ready(self) -> bool:
        """
        检查数据源链接是否就绪
        """
        ...

    @abstractmethod
    async def new_getter(self, *args, **kwargs) -> "BaseGetter":
        """
        new a data getter
        """
        ...

    @abstractmethod
    async def new_writer(self, *args, **kwargs) -> "BaseWriter":
        """
        new a data writer
        """
        ...


class BaseGetter(ABC, LogKit):
    # src_name: 用于输出日志时指示读取源的名称
    src_name: str
    
    def __init__(self, src_name: str = "", batch_size: int = None, max_size: int = 0):
        # 以下参数为各个数据源都会有的参数，故抽出来放在这里
        self.src_name = src_name
        self.done_cnt = 0
        self.max_size = max_size
        self.total_cnt = 0

        self.finish_rate = 0
        self.first_fetch_ts = None
        self.last_fetch_ts = None
        self.batch_size = batch_size
    
    def __aiter__(self):
        """
        base getter should implement __aiter__
        """
        return self

    async def __anext__(self):
        """
        构建各个数据源的生成器的同时计算进度等指示数据，便于记录成日志
        :return: 
        """
        await self._get_total_count()

        if not self.first_fetch_ts:
            self.first_fetch_ts = time()
            self.last_fetch_ts = self.first_fetch_ts

        next_lst = await self._get_next_lst()

        curr_ts = time()
        cost_time = curr_ts - self.last_fetch_ts
        self.last_fetch_ts = curr_ts
        if not next_lst:
            msg = self.getter_finish_msg_tmpl.format(self.src_name, self.done_cnt,
                                                     FuncSet.x2humansTime(time() - self.first_fetch_ts))
            self.logger.info(msg)
            raise StopAsyncIteration
        # fetch stats
        # 已读取数量
        self.done_cnt += len(next_lst)
        # 读取完成率
        self.finish_rate = self.done_cnt / self.total_cnt
        finish_rate_str = "%.2f" % (self.finish_rate * 100)
        # 每一秒的获取数量
        fetch_cnt_per_sec = self.done_cnt / (time() - self.first_fetch_ts)
        # 剩余时间的计算
        left_time = (self.total_cnt - self.done_cnt) / fetch_cnt_per_sec
        
        msg = self.getter_batch_msg_tmpl.format(self.src_name, len(next_lst), self.done_cnt, self.total_cnt,
                                                finish_rate_str,
                                                FuncSet.x2humansTime(cost_time), FuncSet.x2humansTime(left_time))
        self.logger.info(msg)

        return next_lst
    
    @abstractmethod
    async def _get_total_count(self):
        ...

    @abstractmethod
    async def _get_next_lst(self):
        ...

   
class BaseWriter(ABC, LogKit):
    # dst_name: 用于输出日志时指示写入源的名称
    dst_name: str

    def __init__(self):
        self.written = 0

    async def write(self, lst: List[Any], *args, **kwargs):
        try:
            before_write_ts = time()
            await self._handle_lst(lst, *args, **kwargs)
            cost_time = time() - before_write_ts
            self.written += len(lst)
            self.logger.info(self.writer_batch_msg_tmpl.format(self.dst_name, len(lst), self.written,
                                                               FuncSet.x2humansTime(cost_time)))
        except Exception as e:
            self.logger.error(e)

    @abstractmethod
    async def _handle_lst(self, lst: List[Any], *args, **kwargs):
        ...
