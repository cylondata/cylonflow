import inspect
import logging

from pycylon import CylonEnv
from pycylon.net.gloo_config import GlooStandaloneConfig

logger = logging.getLogger(__name__)


class CylonRayActor:
    """
    Actor class at the workers
    """

    def __init__(self, world_rank=0, world_size=1) -> None:
        self.rank = world_rank
        self.world_size = world_size

        self.executable = None
        self.cylon_env = None

    def execute(self, func):
        """Executes an arbitrary function on self."""
        print("HERE!")
        print(inspect.getfullargspec(func))
        return func(self, self.cylon_env)

    def start_executable(self,
                         executable_cls: type = None,
                         executable_args: list = None,
                         executable_kwargs: dict = None):
        executable_args = executable_args or []
        executable_kwargs = executable_kwargs or {}
        if executable_cls:
            self.executable = executable_cls(*executable_args,
                                             **executable_kwargs)

        config = GlooStandaloneConfig(rank=self.rank, world_size=self.world_size)
        config.set_file_store_path('/tmp/gloo')
        config.set_store_prefix('foo')
        self.cylon_env = CylonEnv(config=config, distributed=True)
