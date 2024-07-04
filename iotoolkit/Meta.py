# @Author : taojinmin
# @Time : 2023/2/6 18:32
import asyncio
from abc import ABC, abstractmethod, abstractproperty
from typing import List
from types import MappingProxyType
from urllib.parse import urlparse
from iotoolkit.util import LogKit


class BaseGetter(ABC, LogKit):
    
    @abstractmethod
    def __aiter__(self):
        """
        base getter should implement __aiter__
        """
        return self

    @abstractmethod
    async def __anext__(self):
        """
       base getter __anext__ 's template
       while read getter, batch data will be fetched to memory and 
       logs detail will be print...
       """
        ...

   
class BaseWriter(ABC, LogKit):
    @abstractmethod
    async def write(self, docs: list):
        """
        base writer should implement write method
        """
        ...


class BasePack(ABC):
    origin_conn_obj: dict
    
    def __init__(self, uri=None, host: str = None, port: int = None, username: str = None, password: str = None, db: str = None, *args, **kwargs):
        if not uri and not any([
            host, port, username, password, db
        ]):
            raise ValueError("connection params error!")
        # 优先解析uri
        if uri:
            self.uri_parse = urlparse(uri)
            self.conn_config = MappingProxyType(
                {
                    "host": uri_parse.hostname,
                    "port": uri_parse.port,
                    "username": uri_parse.username,
                    "password": uri_parse.password,
                    "db": uri_parse.path[1:]
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
                    "db": db
                }
            )
            query = "&".join([k + "=" + v for k, v in kwargs.items()])
            uri = self.scheme + "://{username}:{password}@{host}:{port}/{db}".format(**self.conn_config) + "?" + query
            self.uri_parse = urlparse(uri)

    @abstractmethod
    async def _build_connect(self) -> None:
        """
        build connection for dbs
        """

    @abstractmethod
    def is_ready(self) -> bool:
        """
        build connection for dbs
        """
    
    @abstractmethod
    async def new_getter(self, *args, **kwargs) -> BaseGetter:
        """
        new a data getter
        """

    @abstractmethod
    async def new_writer(self, *args, **kwargs) -> BaseWriter:
        """
        new a data writer
        """

    
        
