# @Author : taojinmin
# @Time : 2023/2/8 12:19
import aioredis
import redis

from iotoolkit.util import LogKit, DefaultValue


class RedisPack(LogKit):
    def __init__(self, host: str = None, port: int = DefaultValue.redis_port, password: str = None, name: str = None,
                 encoding: str = DefaultValue.encoding, init_async=True, init_sync=False):
        self.name = name
        self.conn_schema = dict(
            host=host,
            port=port,
            password=password
        )
        print(self.conn_schema)
        self.conn_schema.update(
            encoding=encoding,
            decode_responses=True
        )
        self.init_async = init_async
        self.init_sync = init_sync

        self.async_cli_map = dict()
        self.sync_cli_map = dict()

    async def get_redis_db_cli(self, db: int = 0, sync_type="async"):
        redis_db_cli = None
        if sync_type == "async":
            if not self.async_cli_map.get(db) or not self._is_connection_alive(self.async_cli_map.get(db), sync_type):
                pool = aioredis.ConnectionPool(**self.conn_schema, db=db)
                self.async_cli_map[db] = aioredis.Redis(connection_pool=pool)
            redis_db_cli = self.async_cli_map[db]

        if sync_type == "sync":
            if not self.sync_cli_map.get(db) or not self._is_connection_alive(self.sync_cli_map.get(db), sync_type):
                pool = redis.ConnectionPool(**self.conn_schema, db=db)
                self.sync_cli_map[db] = redis.Redis(connection_pool=pool)
            redis_db_cli = self.sync_cli_map[db]
        return redis_db_cli

    @staticmethod
    async def _is_connection_alive(cli, sync_type="async"):
        if sync_type == "async":
            try:
                return "PONG" == await cli.ping()
            except:
                return False
        elif sync_type == "sync":
            try:
                return "PONG" == cli.ping()
            except:
                return False
        return False

    async def close(self):
        for cli in self.async_cli_map.values():
            try:
                await cli.close()
            except:
                pass
        self.async_cli_map.clear()

        for cli in self.sync_cli_map.values():
            try:
                cli.close()
            except:
                pass
        self.sync_cli_map.clear()
