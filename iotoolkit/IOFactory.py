# @Author : taojinmin
# @Time : 2023/2/6 18:32
from _asyncio import Future
from hashlib import md5
from logging import Logger
from time import time
from abc import ABC, abstractmethod

from motor.motor_asyncio import AsyncIOMotorCursor, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import UpdateOne

from iotoolkit.util import DefaultValue, LogKit, FuncSet


class IOFactory:
    @staticmethod
    def create_mongo_getter(db_cli: AsyncIOMotorDatabase, col: str, query: dict,
                            return_fields: list, batch_size: int = None, max_size: int = None, logger: Logger = None):
        col_obj = db_cli.get_collection(col)
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
        doc_cnt_future = col_obj.estimated_document_count() if not query else col_obj.count_documents(query)
        getter = MongoGetter(col, cursor, batch_size or DefaultValue.getter_batch_size, max_size, doc_cnt_future,
                             logger)
        return getter

    @staticmethod
    def create_mongo_writer(db_cli: AsyncIOMotorDatabase, col: str, write_method: str = None, logger: Logger = None):
        col_obj = db_cli.get_collection(col)
        writer = MongoWriter(col_obj, write_method or DefaultValue.mongo_writer_method, logger)
        return writer


class BaseGetter(ABC):
    @abstractmethod
    def __aiter__(self):
        """
        base getter should implement __aiter__
        """

    @abstractmethod
    def __anext__(self):
        """
         base getter should implement __anext__
        """


class MongoGetter(BaseGetter, LogKit):
    def __init__(self, col: str, cursor: AsyncIOMotorCursor, batch_size: int, max_size: int, doc_cnt_future: Future,
                 logger: Logger = None):
        self.col = col
        self.cursor = cursor
        self.batch_size = batch_size
        self.done_cnt = 0
        self.total_cnt = max_size
        self.finish_rate = 0
        self.doc_cnt_future = doc_cnt_future
        self.new_logger(self.__class__.__name__)
        self.logger = logger or self.logger
        self.first_fetch_ts = None
        self.last_fetch_ts = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.first_fetch_ts:
            self.first_fetch_ts = time()
            self.last_fetch_ts = self.first_fetch_ts
        if not self.total_cnt:
            self.total_cnt = await self.doc_cnt_future
        next_lst = await self.cursor.to_list(length=self.batch_size)
        curr_ts = time()
        cost_time = curr_ts - self.last_fetch_ts
        self.last_fetch_ts = curr_ts
        if not next_lst:
            msg = self.getter_finish_msg_tmpl.format(self.col, self.done_cnt,
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
        msg = self.getter_batch_msg_tmpl.format(self.col, len(next_lst), self.done_cnt, self.total_cnt, finish_rate_str,
                                                FuncSet.x2humansTime(cost_time), FuncSet.x2humansTime(left_time))
        self.logger.info(msg)
        return next_lst


class BaseWriter(ABC):
    @abstractmethod
    def write(self, docs: list):
        """
        base writer should implement write method
        """


class MongoWriter(BaseWriter, LogKit):
    def __init__(self, col_obj: AsyncIOMotorCollection, write_method: str = "insert", logger: Logger = None):
        if write_method not in ["insert", "upsert"]:
            raise ValueError("write method must be one of ['insert', 'upsert']")
        self.col_obj = col_obj
        self.write_method = write_method
        self.written = 0
        self.new_logger(self.__class__.__name__)
        self.logger = logger or self.logger

    async def write(self, docs: list):
        if self.write_method == "insert":
            try:
                before_write_ts = time()
                await self.col_obj.insert_many(docs)
                cost_time = time() - before_write_ts
                self.written += len(docs)
                self.logger.info(self.writer_batch_msg_tmpl.format(self.col_obj.name, len(docs), self.written,
                                                                   FuncSet.x2humansTime(cost_time)))
            except Exception as e:
                self.logger.error(e)
        elif self.write_method == "upsert":
            try:
                ops = list()
                for doc in docs:
                    if "_id" not in doc:
                        _id = md5(str(doc).encode()).hexdigest()
                    else:
                        _id = doc["_id"]
                    ops.append(UpdateOne({"_id": _id}, {"$set": doc}, upsert=True))
                before_write_ts = time()
                await self.col_obj.bulk_write(ops)
                cost_time = time() - before_write_ts
                self.written += len(docs)
                self.logger.info(self.writer_batch_msg_tmpl.format(self.col_obj.name, len(docs), self.written,
                                                                   FuncSet.x2humansTime(cost_time)))

            except Exception as e:
                self.logger.error(e)
