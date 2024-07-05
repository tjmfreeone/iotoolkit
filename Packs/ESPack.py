# @Author : taojinmin
# @Time : 2023/2/8 12:19
# https://elasticsearch-py.readthedocs.io/en/v8.14.0
import aioredis
import redis
import json

from iotoolkit.Packs.Meta import BasePack, BaseGetter, BaseWriter, OriginConnObj
from iotoolkit.util import LogKit, DefaultValue, FuncSet
from iotoolkit.PackManager import pack_manager
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_scan, async_bulk
from types import FunctionType
from typing import List, Any
from time import time


class ESPack(LogKit, BasePack):

    def __init__(self,  hosts=None, **kwargs):
        """
        注意ESPack的构造方法和其他数据库不一样
        """
        self.scheme = "es"
        self.hosts = hosts
        self.kwargs = kwargs
        self.origin_conn_obj = OriginConnObj()
        pack_manager.register_pack(self)

    def is_ready(self):
        return self.origin_conn_obj.cli is not None

    async def _build_connect(self) -> None:
        """
        build connection for dbs
        """
        self.origin_conn_obj.cli = AsyncElasticsearch(hosts=self.hosts, **self.kwargs)

    @FuncSet.ensure_connected
    async def new_getter(self, index_name: str, doc_type: str = "", query: dict = None, batch_size: int = 100, max_size: int = 0):
        return ESGetter(cli=self.origin_conn_obj.cli, index_name=index_name, query = query, batch_size=batch_size, max_size=max_size)

    @FuncSet.ensure_connected
    async def new_writer(self, index_name: str):
        return ESWriter(cli=self.origin_conn_obj.cli, index_name=index_name)


class ESGetter(BaseGetter):
    src_name: str
    scroll_id: str
    
    def __init__(self, cli: AsyncElasticsearch, index_name: str, query: dict = None, doc_type: str = None, batch_size: int = None, max_size: int = 0):
        super().__init__(batch_size=batch_size, max_size=max_size)
        self.index_name = index_name
        self.src_name = index_name
        self.doc_type = doc_type
        self.cli: AsyncElasticsearch = cli
        self.query = query
        self.scroll_id = ""

    async def _get_total_count(self):
        if not self.total_cnt:
            if self.max_size:
                self.total_cnt = self.max_size
            else:
                query_copy = self.query.copy() if self.query else None
                if query_copy and query_copy.get("size"):
                    query_copy.pop("size")
                res = await self.cli.count(index=self.index_name, body=query_copy, doc_type=self.doc_type)
                self.total_cnt = res["count"]

    async def _get_next_lst(self) -> List:
        next_lst = []
        if not self.scroll_id:
            resp = await self.cli.search(index=self.index_name, body=self.query, scroll="5m", size=self.batch_size)
            self.scroll_id = resp["_scroll_id"]
            hits = resp["hits"]["hits"]
            for hit in hits:
                next_lst.append(hit)
            if not next_lst:
                # 没有数据，说明分页到底了
                await self.cli.clear_scroll(scroll_id=self.scroll_id)
            return next_lst

        if self.done_cnt < self.total_cnt:
            resp = await self.cli.scroll(scroll_id=self.scroll_id, scroll="5m")
            self.scroll_id = resp["_scroll_id"]
            hits = resp["hits"]["hits"]
            for hit in hits:
                next_lst.append(hit)

        if not next_lst:
            # 没有数据，说明分页到底了
            await self.cli.clear_scroll(scroll_id=self.scroll_id)
        return next_lst
    
    
class ESWriter(BaseWriter):
    dst_name: str
    
    def __init__(self, cli: AsyncElasticsearch, index_name: str):
        super().__init__()
        self.index_name = index_name
        self.dst_name = index_name
        self.cli = cli

    async def _handle_lst(self, lst: List[Any]):
        async def aiter_func():
            for doc in lst:
                yield {"_index": self.index_name,
                       "doc": doc}
        await async_bulk(self.cli, aiter_func())
        
        
