import logging
from typing import Callable, Any, Optional, List, Dict

import ray

from cylonflow.ray.worker.pool import CylonRayWorkerPool

logger = logging.getLogger(__name__)


class CylonRayExecutor:
    """
    Driver class
    """

    def __init__(self, num_workers, pg_strategy='STRICT_SPREAD'):
        self.num_workers = num_workers
        self.pg_strategy = pg_strategy

        self.remote_worker_pool = None

    def start(self,
              executable_cls: type = None,
              executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None):
        self.remote_worker_pool = ray.remote(CylonRayWorkerPool).remote(self.num_workers, pg_strategy=self.pg_strategy)
        ray.get(self.remote_worker_pool.start.remote(executable_cls=executable_cls,
                                                     executable_args=executable_args,
                                                     executable_kwargs=executable_kwargs))

    def run_cylon(self,
                  fn: Callable[[Any], Any],
                  args: Optional[List] = None,
                  kwargs: Optional[Dict] = None) -> List[Any]:
        return self.remote_worker_pool.run_cylon.remote(fn=fn, args=args, kwargs=kwargs)

    def run(self,
            fn: Callable[[Any], Any],
            args: Optional[List] = None,
            kwargs: Optional[Dict] = None) -> List[Any]:
        return self.remote_worker_pool.run.remote(fn=fn, args=args, kwargs=kwargs)

    def execute(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        return self.remote_worker_pool.execute.remote(fn=fn)

    def execute_cylon(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        return self.remote_worker_pool.execute_cylon.remote(fn=fn)

    def shutdown(self):
        if self.remote_worker_pool:
            self.remote_worker_pool.shutdown.remote()
