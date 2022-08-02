import os
import shutil
from typing import Callable, Any, List, Optional, Dict

from cylonflow.api.actor import CylonGlooFileStoreActor
from cylonflow.api.config import GlooFileStoreConfig
from cylonflow.api.worker import WorkerPool
from distributed import Client, Actor, wait
from distributed.actor import ActorFuture


class DaskWorkerPool(WorkerPool):
    client = None
    num_workers = 0
    worker_addresses: List[str] = None
    workers: List[Actor] = None

    actor_cls = None
    actor_args = None

    def __init__(self, client: Client, num_workers: int) -> None:
        self.client = client
        self.num_workers = num_workers
        self.worker_addresses = list(client.ncores().keys())[0:num_workers]

    def _run_cylon_remote(self,
                          fn: Callable[[Any], Any]) -> List[Any]:
        # Use run_remote for all calls
        # for elastic, start the driver and launch the job
        return [worker.execute_cylon(fn) for worker in self.workers]

    def run_cylon(self, fn: Callable[[Any], Any], args: Optional[List] = None,
                  kwargs: Optional[Dict] = None) -> List[Any]:
        args = args or []
        kwargs = kwargs or {}
        f = lambda self_obj, cylon_env=None: fn(*args, cylon_env=cylon_env, **kwargs)

        a_futures = self._run_cylon_remote(f)
        return self._wait_for_actor_futures(a_futures)

    def _create_workers(self):
        # args = [(i, self.num_workers, *self.actor_args) for i in range(self.num_workers)]
        args = [ list(range(self.num_workers)), [self.num_workers]*self.num_workers]
        for a in self.actor_args:
            args.append([a]*self.num_workers)

        print('args:', args)
        futures = self.client.map(self.actor_cls, *args,
                                  key='cy_create',
                                  workers=self.worker_addresses,
                                  allow_other_workers=False,
                                  actors=True,
                                  pure=False)

        self.workers = [f.result() for f in wait(futures, return_when='ALL_COMPLETED').done]

    def start(self, executable_cls: type = None, executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None) -> None:
        self._create_workers()

        a_futures = [w.start_executable(executable_cls, executable_args, executable_kwargs)
                     for w in self.workers]

        self._wait_for_actor_futures(a_futures)

    def execute_cylon(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        a_futures = self._run_cylon_remote(fn)
        return self._wait_for_actor_futures(a_futures)

    def shutdown(self) -> None:
        del self.workers

    @staticmethod
    def _wait_for_actor_futures(futures: List[ActorFuture]):
        return [f.result() for f in futures]


class DaskFileStoreWorkerPool(DaskWorkerPool):
    def __init__(self, client: Client, num_workers, config: GlooFileStoreConfig = None):
        super().__init__(client, num_workers)
        self.gloo_file_store_path = config.file_store_path

        self.actor_cls = CylonGlooFileStoreActor
        self.actor_args = [config.file_store_path,  # file_store_path
                           config.store_prefix or str(client.id)]  # store_prefix

        os.makedirs(config.file_store_path, exist_ok=True)

    def shutdown(self):
        super().shutdown()

        if os.path.exists(self.gloo_file_store_path):
            shutil.rmtree(self.gloo_file_store_path)


class CylonDaskExecutor:
    client: Client = None
    worker_pool: DaskWorkerPool = None

    def __init__(self, num_workers: int, config, address: str = None,
                 scheduler_file: str = None) -> None:
        self.client = Client(address=address, scheduler_file=scheduler_file)
        self.worker_pool = DaskWorkerPool(self.client, num_workers)

        if isinstance(config, GlooFileStoreConfig):
            self.worker_pool = DaskFileStoreWorkerPool(self.client, num_workers, config)
        else:
            raise ValueError(f'Invalid config type {type(config)}')

    def run_cylon(self, fn: Callable[[Any], Any], args: Optional[List] = None,
                  kwargs: Optional[Dict] = None) -> List[Any]:
        return self.worker_pool.run_cylon(fn, args, kwargs)

    def start(self, executable_cls: type = None, executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None) -> None:
        return self.worker_pool.start(executable_cls, executable_args, executable_kwargs)

    def execute_cylon(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        return self.worker_pool.execute_cylon(fn=fn)

    def shutdown(self):
        return self.worker_pool.shutdown()
