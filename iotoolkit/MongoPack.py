# @Author : taojinmin
# @Time : 2023/2/6 16:59
from functools import wraps
from logging import Logger

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from pymongo import uri_parser

from iotoolkit.IOFactory import BaseGetter, BaseWriter
from iotoolkit.IOFactory import IOFactory
from iotoolkit.util import LogKit


def ensure_connected(method):
    @wraps(method)
    def wrapper(method_this, *args, **kwargs):
        if not method_this.is_ready():
            method_this.logger.info(
                "mongodb[{}]'s connection building...".format(method_this._schema_detail["database"]))
            method_this.build_connect()
        result = method(method_this, *args, **kwargs)
        return result

    return wrapper


class MongoPack(LogKit):
    def __init__(self, schema=None, init_async=True, init_sync=False):
        """
        :param init_async: whether build a connection via motor
        :param init_sync: whether build a connection via pymongo
        """
        self.init_async = init_async
        self.init_sync = init_sync
        self.conn_schema = schema
        self._schema_detail = uri_parser.parse_uri(self.conn_schema) if self.conn_schema else {}

        self.async_conn = None
        self.async_db_cli = None

        self.sync_conn = None
        self.sync_db_cli = None

        self.new_logger(self.__class__.__name__)

    def is_ready(self):
        return any([self.async_db_cli is not None,
                    self.sync_db_cli is not None])

    @ensure_connected
    def get_db_client(self, sync_type="async"):
        """
        :param sync_type: one of `sync` and `async`, default `async`
        """
        if sync_type == "async":
            return self.async_db_cli
        return self.sync_db_cli

    def set_conn_schema(self, schema: str = None, host: str = None, port: int = None, username: str = None,
                        password: str = None, db: str = None, *args, **kwargs) -> None:
        """
        :param schema: schema instruct that what mongodb you are going to connect
                    e.g.: "mongodb://{username}:{password}@{host}:{port}/{db}"
        :param host: mongodb's host
        :param port: mongodb's port
        :param username: auth name
        :param password: auth pwd
        :param db: database's name
        """
        if not schema and not any([host, port, username, password, db]):
            raise ValueError("set conn schema fail cause some args got 'None' value!")
        if schema:
            self.conn_schema = schema
            return
        self.conn_schema = f"mongodb://{username}:{password}@{host}:{port}/{db}".format(username=username,
                                                                                        password=password,
                                                                                        host=host,
                                                                                        port=port,
                                                                                        db=db)
        self._schema_detail = uri_parser.parse_uri(self.conn_schema)

    def build_connect(self) -> None:
        if self.conn_schema:
            db = self.conn_schema.strip().split("/")[-1]
            if self.init_async:
                self.async_conn = AsyncIOMotorClient(self.conn_schema)
                self.async_db_cli = self.async_conn[db]
            if self.init_sync:
                self.sync_conn = MongoClient(self.conn_schema)
                self.sync_db_cli = self.sync_conn[db]
        else:
            raise ConnectionError("conn schema is None")

    @ensure_connected
    def new_getter(self, col: str, query: dict = {}, return_fields: list = [], batch_size: int = None,
                   max_size: int = None, logger: Logger = None, *args, **kwargs) -> BaseGetter:
        """
        :param col: collection's name
        :param query: query body
        :param return_fields: projection fields
        :param max_size: return-data's max size
        :param batch_size: size of batch data
        :param logger: Logger instance
        :return: async iter
        """
        getter = IOFactory.create_mongo_getter(self.async_db_cli, col, query, return_fields,
                                               batch_size, max_size, logger)
        return getter

    @ensure_connected
    def new_writer(self, col: str, write_method: str = None, logger: Logger = None) -> BaseWriter:
        writer = IOFactory.create_mongo_writer(self.async_db_cli, col, write_method, logger)
        return writer
