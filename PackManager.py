# @Author : taojinmin
# @Time : 2024/7/3 21:55
import atexit
import asyncio
from iotoolkit.util import LogKit


class PackManager(LogKit):
    _running_packs = dict()

    def register_pack(self, pack_instance: "BasePack"):
        self._running_packs[id(pack_instance)] = pack_instance

    def unregister_pack(self, pack_instance: "BasePack"):
        self._running_packs.pop(id(pack_instance))

    @staticmethod
    def _get_class_name(obj):
        return obj.__class__.__name__

    async def close_pack_conn_obj(self, pack_instance: "BasePack"):
        if pack_instance.scheme.startswith("mongodb"):
            # pack_instance.origin_conn_obj.cli.close()
            pass

        elif pack_instance.scheme == "es":
            await pack_instance.origin_conn_obj.cli.close()

        elif pack_instance.scheme == "redis":
            if pack_instance.origin_conn_obj.cli:
                await pack_instance.origin_conn_obj.cli.close()
                pack_instance.origin_conn_obj.pool.reset()

        elif pack_instance.scheme == "mysql":
            pack_instance.origin_conn_obj.pool.close()
        self.logger.info(f"close connection of {pack_instance.scheme}")

    def close_all(self):
        loop = asyncio.get_event_loop()
        for _, pack_instance in self._running_packs.items():
            loop.run_until_complete(self.close_pack_conn_obj(pack_instance))


pack_manager = PackManager()
atexit.register(pack_manager.close_all)



