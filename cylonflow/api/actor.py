import logging
from abc import ABC, abstractmethod

from pycylon import CylonEnv
from pycylon.net.gloo_config import GlooStandaloneConfig

from cylonflow.api.config import GlooFileStoreConfig

logger = logging.getLogger(__name__)


class CylonActor(ABC):
    """
    Actor class at the workers
    """

    def __init__(self, world_rank=0, world_size=1) -> None:
        self.rank = world_rank
        self.world_size = world_size

        self.executable = None
        self.cylon_env = None

    @abstractmethod
    def start_cylon_env(self):
        pass

    def shutdown(self):
        if self.executable:
            del self.executable

        del self.cylon_env

    def execute_cylon(self, func):
        """Executes an arbitrary function on self."""
        return func(self.executable, cylon_env=self.cylon_env)

    def execute(self, func):
        """Executes an arbitrary function on self."""
        return func(self.executable)

    def start_executable(self,
                         executable_cls: type = None,
                         executable_args: list = None,
                         executable_kwargs: dict = None):
        executable_args = executable_args or []
        executable_kwargs = executable_kwargs or {}
        if executable_cls:
            self.executable = executable_cls(*executable_args,
                                             **executable_kwargs)

        if self.cylon_env is None:
            self.start_cylon_env()


class CylonGlooFileStoreActor(CylonActor):
    def __init__(self, world_rank=0, world_size=1, config: GlooFileStoreConfig = None) -> None:
        super().__init__(world_rank, world_size)
        if not config:
            raise ValueError('config can not be None')
        self.config = config
        self.file_store_path = config.file_store_path or '/tmp/gloo'
        self.store_prefix = config.store_prefix or 'cylon_gloo'

    def start_cylon_env(self):
        comm_config = GlooStandaloneConfig(rank=self.rank, world_size=self.world_size)
        comm_config.set_file_store_path(self.file_store_path)
        comm_config.set_store_prefix(self.store_prefix)

        if self.config.tcp_iface:
            comm_config.set_tcp_iface(self.config.tcp_iface)

        if self.config.tcp_host_name:
            comm_config.set_tcp_host_name(self.config.tcp_host_name)

        if self.config.tcp_ai_family:
            comm_config.set_tcp_ai_family(self.config.tcp_ai_family)

        self.cylon_env = CylonEnv(config=comm_config, distributed=self.world_size > 1)
