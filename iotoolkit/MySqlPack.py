# @Author : taojinmin
# @Time : 2023/2/6 16:59
# aiomysql doc: https://aiomysql.readthedocs.io/
import asyncio
import inspect
from functools import wraps
from logging import Logger
from time import time

import aiomysql
import pymysql
from types import MappingProxyType
from typing import List, Dict
import traceback

from iotoolkit.Meta import BasePack, BaseGetter, BaseWriter
from iotoolkit.util import DefaultValue, LogKit, FuncSet
from sql_metadata import Parser
from collections import OrderedDict



def ensure_connected(method):
    @wraps(method)
    async def wrapper(method_this: BasePack, *args, **kwargs):
        if not inspect.iscoroutinefunction(method):
            raise ValueError("method must be coroutine function...")
        if not method_this.is_ready():
            method_this.logger.info(
                "mysql[{}]'s connection building...".format(method_this.conn_config.get("db")))
            await method_this._build_connect()
        result = await method(method_this, *args, **kwargs)
        return result
    return wrapper


class MySqlPack(LogKit, BasePack):
    pack_name = "mysql"
    origin_conn_obj = {
        "pool": None,
    }

    def __init__(self,
                 host="",
                 port="",
                 username="",
                 password="",
                 db="",
                 ):
        """
        """
        if not any([
            host, port, username, password, db
        ]):
            raise ValueError("connection params error!")
        self.conn_config = MappingProxyType(
            {
                "host": host,
                "port": int(port),
                "user": username,
                "password": password,
                "db": db
            }
        )

    async def _build_connect(self):
        self.origin_conn_obj["pool"] = await aiomysql.create_pool(cursorclass=pymysql.cursors.DictCursor,
                                                                  autocommit=False,
                                                                  **self.conn_config)

    def is_ready(self):
        return self.origin_conn_obj["pool"] is not None

    @ensure_connected
    async def new_getter(self, select_sql: str = "", table: str = "", return_fields: List[str] = None, where: str = "", offset: int = 0, limit: int = 0, batch_size: int = 10) -> BaseGetter:
        """
        :param select_sql: raw sql
        :param table: table name
        :param return_fields: projection fields
        :param where: query
        :param offset: skip size
        :param limit: limit size
        :param batch_size: batch size
        :return: async iter
        """
        getter = MySqlGetter(pool=self.origin_conn_obj["pool"],
                             select_sql=select_sql, table=table,
                             return_fields=return_fields, where=where,
                             offset=offset, limit=limit, batch_size=batch_size)
        return getter

    @ensure_connected
    async def new_writer(self, table: str = "") -> BaseWriter:
        """
        create a writer object
        :param table: 表名，写入前必须把表建好
        """
        conn = await self.origin_conn_obj["pool"].acquire()
        cursor = await conn.cursor(aiomysql.Cursor)
        await cursor.execute("show tables;")

        exists_tables = await cursor.fetchall()
        await cursor.close()
        self.origin_conn_obj["pool"].release(conn)

        if table not in {t[0] for t in exists_tables}:
            raise ValueError("table is not exists.")

        writer = MySqlWriter(pool=self.origin_conn_obj["pool"], table=table)
        return writer


class MySqlGetter(BaseGetter, LogKit):
    _pool: aiomysql.pool = None
    _conn: aiomysql.connection = None
    _cursor: aiomysql.DictCursor = None

    def __init__(self, pool: aiomysql.pool = None,
                 select_sql: str = "", table: str = "",
                 return_fields: List[str] = None,
                 where: str = "", offset: int = 0, limit: int = 0,
                 batch_size: int = None):
        self._pool = pool

        # select_sql's process
        if not select_sql and table != "":
            fields_desc = "*" if not return_fields else ", ".join(return_fields)
            select_sql = f"SELECT {fields_desc} FROM {table}"
        else:
            raise ValueError("Table name must be specified")

        select_sql = select_sql.rstrip(";")
        if isinstance(where, str) and where != "" and "WHERE" not in select_sql.upper():
            select_sql += " WHERE " + where
        if isinstance(offset, int) and offset != 0 and "OFFSET" not in select_sql.upper():
            select_sql += " OFFSET " + str(offset)
        if isinstance(limit, int) and limit != 0 and "OFFSET" not in select_sql.upper():
            select_sql += " LIMIT " + str(limit)

        self.select_sql = select_sql + ";"
        self.logger.info("query sql: "+self.select_sql)

        self.sql_parser = Parser(select_sql)
        self.table = table or self.sql_parser.tables[0]
        self.batch_size = batch_size
        self.done_cnt = 0
        self.max_size = limit
        self.total_cnt = 0
        self.finish_rate = 0
        self.first_fetch_ts = None
        self.last_fetch_ts = None
        self.has_execute = False

    async def _init_conn_coro(self):
        if not self._cursor:
            self._conn = await self._pool.acquire()
            self._cursor = await self._conn.cursor(aiomysql.DictCursor)

    async def release(self):
        await self._cursor.close()
        self._pool.release(self._conn)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await self._init_conn_coro()

        if not self.first_fetch_ts:
            self.first_fetch_ts = time()
            self.last_fetch_ts = self.first_fetch_ts

        if not self.has_execute:
            await self._cursor.execute(self.select_sql)
            self.has_execute = True

        if not self.total_cnt:
            self.total_cnt = self._cursor.rowcount
        next_lst = await self._cursor.fetchmany(self.batch_size)
        curr_ts = time()
        cost_time = curr_ts - self.last_fetch_ts
        self.last_fetch_ts = curr_ts
        if not next_lst:
            msg = self.getter_finish_msg_tmpl.format(self.table, self.done_cnt,
                                                     FuncSet.x2humansTime(time() - self.first_fetch_ts))
            self.logger.info(msg)
            await self.release()
            raise StopAsyncIteration
        # fetch stats
        self.done_cnt += len(next_lst)
        self.finish_rate = self.done_cnt / self.total_cnt
        finish_rate_str = "%.2f" % (self.finish_rate * 100)
        fetch_cnt_per_sec = self.done_cnt / (time() - self.first_fetch_ts)
        # fetch_cnt_per_sec = len(next_lst) / cost_time
        left_time = (self.total_cnt - self.done_cnt) / fetch_cnt_per_sec
        msg = self.getter_batch_msg_tmpl.format(self.table, len(next_lst), self.done_cnt, self.total_cnt,
                                                finish_rate_str,
                                                FuncSet.x2humansTime(cost_time), FuncSet.x2humansTime(left_time))
        self.logger.info(msg)
        return next_lst


class MySqlWriter(BaseWriter, LogKit):
    _pool = None
    _conn = None
    _cursor = None

    def __init__(self, pool: aiomysql.pool, table: str = ""):
        self._pool = pool
        self.table = table
        self.written = 0

    async def _init_conn_coro(self):
        if not self._cursor:
            self._conn = await self._pool.acquire()
            self._cursor = await self._conn.cursor(aiomysql.DictCursor)

    async def release(self):
        await self._cursor.close()
        self._pool.release(self._conn)

    async def write(self, docs: List[Dict]):
        await self._init_conn_coro()
        try:
            before_write_ts = time()
            docs = list(map(OrderedDict, docs))
            table_head = tuple(OrderedDict(docs[0]).keys())
            values_desc = ", ".join(["%s"] * len(table_head))
            table_head_desc = ", ".join(table_head)
            values_list = list(map(tuple, [doc.values() for doc in docs]))
            
            write_sql = f"INSERT INTO {self.table} ({table_head_desc}) VALUES ({values_desc});"
            await self._cursor.executemany(write_sql, values_list)
            await self._conn.commit()
            cost_time = time() - before_write_ts
            self.written += len(docs)
            self.logger.info("write sql: " + write_sql)
            self.logger.info(self.writer_batch_msg_tmpl.format(self.table, len(docs), self.written,
                                                               FuncSet.x2humansTime(cost_time)))
        except Exception as e:
            print(traceback.format_exc())
            self.logger.error(e)

