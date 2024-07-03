# @Author : taojinmin
# @Time : 2023/2/6 18:32
import asyncio
from abc import ABC, abstractmethod, abstractproperty
from typing import List
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

   
class BaseWriter(ABC):
    @abstractmethod
    async def write(self, docs: list):
        """
        base writer should implement write method
        """
        ...


class BasePack(ABC):
    pack_name: str
    origin_conn_obj: dict
    
    @abstractmethod
    def new_getter(self, *args, **kwargs) -> BaseGetter:
        """
        new a data getter
        """

    @abstractmethod
    def new_writer(self, *args, **kwargs) -> BaseWriter:
        """
        new a data writer
        """

    @abstractmethod
    def _build_connect(self) -> None:
        """
        build connection for dbs
        """
        
