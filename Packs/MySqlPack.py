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
from typing import List, Dict
import traceback

from iotoolkit.Packs.Meta import BasePack, BaseGetter, BaseWriter
from iotoolkit.util import DefaultValue, LogKit, FuncSet
from sql_metadata import Parser
from collections import OrderedDict


class MySqlPack(LogKit, BasePack):
    
    def __init__(self, *args, **kwargs):
        self.scheme = "mysql"
        BasePack.__init__(self, *args, **kwargs)

    async def _build_connect(self):
        conn_config_copy = self.conn_config.copy()
        conn_config_copy["user"] = conn_config_copy.pop("username")
        self.origin_conn_obj.pool = await aiomysql.create_pool(cursorclass=pymysql.cursors.DictCursor,
                                                                  autocommit=False,
                                                                  **conn_config_copy)

    def is_ready(self):
        return self.origin_conn_obj.pool is not None

    @FuncSet.ensure_connected
    async def new_getter(self, select_sql: str = "", table: str = "", return_fields: List[str] = None, where: str = "", offset: int = 0, limit: int = 0, batch_size: int = 100) -> BaseGetter:
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
        getter = MySqlGetter(pool=self.origin_conn_obj.pool,
                             select_sql=select_sql, table=table,
                             return_fields=return_fields, where=where,
                             offset=offset, limit=limit, batch_size=batch_size)
        return getter

    @FuncSet.ensure_connected
    async def new_writer(self, table: str = "") -> BaseWriter:
        """
        create a writer object
        :param table: 表名，写入前必须把表建好
        """
        conn = await self.origin_conn_obj.pool.acquire()
        cursor = await conn.cursor(aiomysql.Cursor)
        await cursor.execute("show tables;")

        exists_tables = await cursor.fetchall()
        await cursor.close()
        self.origin_conn_obj.pool.release(conn)

        if table not in {t[0] for t in exists_tables}:
            raise ValueError("table is not exists.")

        writer = MySqlWriter(pool=self.origin_conn_obj.pool, table=table)
        return writer


class MySqlGetter(BaseGetter):
    _pool: aiomysql.pool = None
    _conn: aiomysql.connection = None
    # 读取数据时需要保持cursor对象为同一个, 不使用async with方式实例化
    _cursor: aiomysql.DictCursor = None
    src_name: str

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
        self.src_name = self.table
        
        super().__init__(batch_size=batch_size, max_size=limit)
        self.has_execute = False

    async def _init_conn_coro(self):
        if not self._cursor:
            self._conn = await self._pool.acquire()
            self._cursor = await self._conn.cursor(aiomysql.DictCursor)

    async def release(self):
        await self._cursor.close()
        self._pool.release(self._conn)

    async def __anext__(self):
        await self._init_conn_coro()
        if not self.has_execute:
            await self._cursor.execute(self.select_sql)
            self.has_execute = True
        await self._get_total_count()
        return await super().__anext__()
    
    async def _get_total_count(self):
        if not self.total_cnt:
            self.total_cnt = self._cursor.rowcount

    async def _get_next_lst(self) -> List:
        next_lst = await self._cursor.fetchmany(self.batch_size)
        return next_lst
        
        
class MySqlWriter(BaseWriter):
    _pool = None
    dst_name: str

    def __init__(self, pool: aiomysql.pool, table: str = ""):
        super().__init__()
        self._pool = pool
        self.table = table
        self.dst_name = table

    async def write(self, lst: List[Dict]):
        async with self._pool.acquire() as conn:
            # 使用async with 方式 获取到链接以便自动回收，避免链接数过多 
            await super().write(lst, conn)
            
    async def _handle_lst(self, lst, conn):
        docs = list(map(OrderedDict, lst))
        table_head = tuple(OrderedDict(docs[0]).keys())
        values_desc = ", ".join(["%s"] * len(table_head))
        table_head_desc = ", ".join(table_head)
        values_list = list(map(tuple, [doc.values() for doc in docs]))

        write_sql = f"INSERT INTO {self.table} ({table_head_desc}) VALUES ({values_desc});"

        async with conn.cursor(aiomysql.DictCursor) as cursor:
            await cursor.executemany(write_sql, values_list)
            await conn.commit()

