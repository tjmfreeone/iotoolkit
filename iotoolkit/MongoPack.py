# @Author : taojinmin
# @Time : 2023/2/6 16:59
import inspect

from time import time
from hashlib import md5
from functools import wraps
from logging import Logger

from motor.motor_asyncio import AsyncIOMotorCursor, AsyncIOMotorCollection, AsyncIOMotorDatabase, AsyncIOMotorClient
from pymongo import MongoClient
from pymongo import uri_parser

from iotoolkit.Meta import BasePack, BaseGetter, BaseWriter
from iotoolkit.util import DefaultValue, LogKit, FuncSet
from typing import List
from types import MappingProxyType, DynamicClassAttribute


def ensure_connected(method):
    @wraps(method)
    def wrapper(method_this: BasePack, *args, **kwargs):
        if not method_this.is_ready():
            method_this.logger.info(
                f"{method_this.pack_name}[{method_this.conn_config['db']}]'s connection building...")
            method_this._build_connect()
        result = method(method_this, *args, **kwargs)
        return result
    return wrapper


class MongoPack(LogKit, BasePack):
    pack_name = "mongodb"
    origin_conn_obj = {
        "db_cli": None
    }
    
    def __init__(self, schema=None, host: str = None, port: int = None, username: str = None,
                        password: str = None, db: str = None):
        """
        :param init_async: whether build a connection via motor
        :param init_sync: whether build a connection via pymongo
        """
        
        if not schema:
            self.conn_schema = f"mongodb://{username}:{password}@{host}:{port}/{db}".format(username=username,
                                                                                            password=password,
                                                                                            host=host,
                                                                                            port=port,
                                                                                            db=db)
        else:
            self.conn_schema = schema
        parse_result = uri_parser.parse_uri(self.conn_schema)
        self.conn_config = MappingProxyType({
            "host": host,
            "port": port,
            "username": parse_result["username"],
            "password": parse_result["password"],
            "db": parse_result["database"]
        })
        
    def is_ready(self):
        return self.origin_conn_obj["db_cli"] is not None

    def _build_connect(self) -> None:
        if self.conn_schema:
            db = self.conn_schema.strip().split("/")[-1]
            self._async_conn = AsyncIOMotorClient(self.conn_schema)
            self.origin_conn_obj["db_cli"] = self._async_conn[db]
        else:
            raise ConnectionError("conn schema is None")

    @ensure_connected
    def new_getter(self, col: str, query: dict = None, return_fields: list = None, batch_size: int = 100,
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

        col_obj = self.origin_conn_obj["db_cli"].get_collection(col)
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

    @ensure_connected
    def new_writer(self, col: str, write_method: str = None) -> BaseWriter:
        """
        create a writer object
        :param col: collection's name
        :param write_method: insert, upsert or insertButNotUpdate
        """
        col_obj = self.origin_conn_obj["db_cli"].get_collection(col)
        writer = MongoWriter(col_obj, write_method or DefaultValue.mongo_writer_method)
        return writer


class MongoGetter(BaseGetter):
    def __init__(self, col_obj, cursor: AsyncIOMotorCursor, query, batch_size: int = None, max_size: int = 0):
        self.col_obj = col_obj
        self.cursor = cursor
        self.query = query
        self.batch_size = batch_size
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
            await self.get_total_count()
        if not self.first_fetch_ts:
            self.first_fetch_ts = time()
            self.last_fetch_ts = self.first_fetch_ts
        next_lst = await self.cursor.to_list(length=self.batch_size)
        curr_ts = time()
        cost_time = curr_ts - self.last_fetch_ts
        self.last_fetch_ts = curr_ts
        if not next_lst:
            msg = self.getter_finish_msg_tmpl.format(self.col_obj.name, self.done_cnt,
                                                     FuncSet.x2humansTime(time() - self.first_fetch_ts))
            self.logger.info(msg)
            await self.cursor.close()
            raise StopAsyncIteration
        # fetch stats
        self.done_cnt += len(next_lst)
        self.finish_rate = self.done_cnt / self.total_cnt
        finish_rate_str = "%.2f" % (self.finish_rate * 100)
        fetch_cnt_per_sec = self.done_cnt / (time() - self.first_fetch_ts)
        # fetch_cnt_per_sec = len(next_lst) / cost_time
        left_time = (self.total_cnt - self.done_cnt) / fetch_cnt_per_sec
        msg = self.getter_batch_msg_tmpl.format(self.col_obj.name, len(next_lst), self.done_cnt, self.total_cnt,
                                                finish_rate_str,
                                                FuncSet.x2humansTime(cost_time), FuncSet.x2humansTime(left_time))
        self.logger.info(msg)
        return next_lst
    
    async def get_total_count(self):
        if self.max_size:
            self.total_cnt = self.max_size
        elif not self.query:
            self.total_cnt = await self.col_obj.estimated_document_count() 
        else: 
            self.total_cnt = await col_obj.count_documents(query)


class MongoWriter(BaseWriter, LogKit):
    def __init__(self, col_obj: AsyncIOMotorCollection, write_method: str = "insert"):
        if write_method not in ["insert", "insertButNotUpdate", "upsert"]:
            raise ValueError("write method must be one of ['insert', 'insertButNotUpdate', 'upsert']")
        self.col_obj = col_obj
        self.write_method = write_method
        self.written = 0

    async def write(self, docs: list):
        if self.write_method == "insert":
            try:
                for doc in docs:
                    if "_id" not in doc:
                        doc["_id"] = md5(str(doc).encode()).hexdigest()
                before_write_ts = time()
                await self.col_obj.insert_many(docs)
                cost_time = time() - before_write_ts
                self.written += len(docs)
                self.logger.info(self.writer_batch_msg_tmpl.format(self.col_obj.name, len(docs), self.written,
                                                                   FuncSet.x2humansTime(cost_time)))
            except Exception as e:
                self.logger.error(e)
        elif self.write_method in ["insertButNotUpdate", "upsert"]:
            set_op = "$set" if self.write_method == "upsert" else "$setOnInsert"
            try:
                ops = list()
                for doc in docs:
                    if "_id" not in doc:
                        _id = md5(str(doc).encode()).hexdigest()
                    else:
                        _id = doc["_id"]
                    ops.append(UpdateOne({"_id": _id}, {set_op: doc}, upsert=True))
                before_write_ts = time()
                await self.col_obj.bulk_write(ops)
                cost_time = time() - before_write_ts
                self.written += len(docs)
                self.logger.info(self.writer_batch_msg_tmpl.format(self.col_obj.name, len(docs), self.written,
                                                                   FuncSet.x2humansTime(cost_time)))

            except Exception as e:
                self.logger.error(e)