# @Author : taojinmin
# @Time : 2023/2/6 16:59
import inspect
import traceback
from time import time
from hashlib import md5
from logging import Logger

from motor.motor_asyncio import AsyncIOMotorCursor, AsyncIOMotorCollection, AsyncIOMotorDatabase, AsyncIOMotorClient

from iotoolkit.Packs.Base import BasePack, BaseGetter, BaseWriter, OriginConnObj
from iotoolkit.util import DefaultValue, LogKit, FuncSet
from typing import List, Any
from types import MappingProxyType, DynamicClassAttribute


class MongoPack(LogKit, BasePack):

    def __init__(self, *args, **kwargs):
        self.scheme = "mongodb"
        BasePack.__init__(self, *args, **kwargs)
        
    def is_ready(self):
        return self.origin_conn_obj.cli is not None

    async def _build_connect(self) -> None:
        if self.conn_config:
            db = self.conn_config["db"]
            self._async_conn = AsyncIOMotorClient(self.uri_parse.geturl())
            self.origin_conn_obj.cli = self._async_conn[db]
        else:
            raise ConnectionError("conn_config is None")

    @FuncSet.ensure_connected
    async def new_getter(self, col: str, query: dict = None, return_fields: list = None, batch_size: int = 100,
                         max_size: int = None, reverse: bool = False, *args, **kwargs) -> BaseGetter:
        """
        :param col: collection's name
        :param query: query body
        :param return_fields: projection fields
        :param max_size: return-data's max size
        :param batch_size: size of batch data
        :param reverse: whether read in reverse order
        :return: async iter
        """
        if return_fields is None:
            return_fields = list()

        col_obj = self.origin_conn_obj.cli.get_collection(col)
        return_fields_dic = {field: 1 for field in return_fields}
        kwargs = dict(
            filter=query,
            projection=return_fields_dic,
            limit=max_size
        )
        kwargs = {k: v for k, v in kwargs.items() if v}

        if kwargs:
            cursor = col_obj.find(**kwargs)
        else:
            cursor = col_obj.find()
        if reverse:
            cursor = cursor.sort([("$natural", -1)])
        getter = MongoGetter(col_obj, cursor, query=query, batch_size=batch_size, max_size=max_size)
        return getter

    @FuncSet.ensure_connected
    async def new_writer(self, col: str, write_method: str = None) -> BaseWriter:
        """
        create a writer object
        :param col: collection's name
        :param write_method: insert, upsert or insertButNotUpdate
        """
        col_obj = self.origin_conn_obj.cli.get_collection(col)
        writer = MongoWriter(col_obj, write_method or DefaultValue.mongo_writer_method)
        return writer


class MongoGetter(BaseGetter):
    src_name: str
    
    def __init__(self, col_obj, cursor: AsyncIOMotorCursor, query, batch_size: int = None, max_size: int = 0):
        super().__init__(batch_size=batch_size, max_size=max_size)
        self.col_obj = col_obj
        self.cursor = cursor
        self.query = query
        self.src_name = self.col_obj.name

    async def _get_total_count(self):
        if not self.total_cnt:
            if self.max_size:
                self.total_cnt = self.max_size
            elif not self.query:
                self.total_cnt = await self.col_obj.estimated_document_count()
            else:
                self.total_cnt = await col_obj.count_documents(query)

    async def _get_next_lst(self) -> List:
        return await self.cursor.to_list(length=self.batch_size)


class MongoWriter(BaseWriter):
    dst_name: str
    
    def __init__(self, col_obj: AsyncIOMotorCollection, write_method: str = "insert"):
        super().__init__()
        if write_method not in ["insert", "insertButNotUpdate", "upsert"]:
            raise ValueError("write method must be one of ['insert', 'insertButNotUpdate', 'upsert']")
        self.col_obj = col_obj
        self.write_method = write_method
        self.dst_name = self.col_obj.name

    async def _handle_lst(self, lst: List[Any]):
        try:
            if self.write_method == "insert":
                for doc in lst:
                    if "_id" not in doc:
                        doc["_id"] = md5(str(doc).encode()).hexdigest()
                await self.col_obj.insert_many(lst)
            elif self.write_method in ["insertButNotUpdate", "upsert"]:
                set_op = "$set" if self.write_method == "upsert" else "$setOnInsert"
                ops = list()
                for doc in lst:
                    if "_id" not in doc:
                        _id = md5(str(doc).encode()).hexdigest()
                    else:
                        _id = doc["_id"]
                    ops.append(UpdateOne({"_id": _id}, {set_op: doc}, upsert=True))
                await self.col_obj.bulk_write(ops)
        except Exception as e:
            self.logger.error(e)
            
