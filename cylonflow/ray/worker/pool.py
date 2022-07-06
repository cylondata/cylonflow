import logging
import os.path
import shutil
from abc import ABC, abstractmethod
from typing import Callable, Any, Optional, List, Dict

import ray

from cylonflow.ray.worker.actor import CylonRayFileStoreActor
from cylonflow.ray.worker.config import GlooFileStoreConfig

logger = logging.getLogger(__name__)


class CylonRayWorkerPool(ABC):
    """
    Acts as the remote object that dispatches tasks/ actors to workers.
    """

    def __init__(self, num_workers, pg_strategy='STRICT_SPREAD', pg_timeout=100):
        self.num_workers = num_workers
        self.pg_strategy = pg_strategy
        self.pg_timeout = pg_timeout

        self.actor_cls = None
        self.actor_kwargs = None
        self.workers = None
        self.placement_group = None

    def _create_placement_group(self):
        """
        create a placement group with {CPU:1} bundles
        :return:
        """
        bundles = [{"CPU": 1} for _ in range(self.num_workers)]
        pg = ray.util.placement_group(bundles, strategy=self.pg_strategy)
        logger.debug("Waiting for placement group to start.")
        ready, _ = ray.wait([pg.ready()], timeout=self.pg_timeout)
        if ready:
            logger.debug("Placement group has started.")
        else:
            raise TimeoutError("Placement group creation timed out. Make sure "
                               "your cluster either has enough resources or use "
                               "an autoscaling cluster. Current resources "
                               "available: {}, resources requested by the "
                               "placement group: {}".format(ray.available_resources(),
                                                            pg.bundle_specs))

        return pg

    def _create_workers(self):
        self.placement_group = self._create_placement_group()

        self.workers = []
        for idx in range(self.num_workers):
            actor = ray.remote(self.actor_cls)
            actor_with_opts = actor.options(num_cpus=1,
                                            placement_group_capture_child_tasks=False,
                                            placement_group=self.placement_group,
                                            placement_group_bundle_index=idx)
            worker = actor_with_opts.remote(world_rank=idx, world_size=self.num_workers,
                                            **self.actor_kwargs)
            self.workers.append(worker)

    def _run_remote(self,
                    fn: Callable[[Any], Any]) -> List[Any]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function that can be executed with arbitrary
                args and keyword arguments.

        Returns:
            list: List of ObjectRefs that you can run `ray.get` on to
                retrieve values.
        """
        # Use run_remote for all calls
        # for elastic, start the driver and launch the job
        return [worker.execute.remote(fn) for worker in self.workers]

    def _run_cylon_remote(self,
                          fn: Callable[[Any], Any]) -> List[Any]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function that can be executed with arbitrary
                args and keyword arguments.

        Returns:
            list: List of ObjectRefs that you can run `ray.get` on to
                retrieve values.
        """
        # Use run_remote for all calls
        # for elastic, start the driver and launch the job
        return [worker.execute_cylon.remote(fn) for worker in self.workers]

    def start(self,
              executable_cls: type = None,
              executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None):
        self._create_workers()

        start_futures = [
            w.start_executable.remote(executable_cls, executable_args, executable_kwargs)
            for w in self.workers]
        ray.get(start_futures)

    def run_cylon(self,
                  fn: Callable[[Any], Any],
                  args: Optional[List] = None,
                  kwargs: Optional[Dict] = None) -> List[Any]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function that can be executed with arbitrary
                args and keyword arguments.
            args: List of arguments to be passed into the target function.
            kwargs: Dictionary of keyword arguments to be
                passed into the target function.

        Returns:
            Deserialized return values from the target function.
        """
        args = args or []
        kwargs = kwargs or {}
        f = lambda self_obj, cylon_env=None: fn(*args, cylon_env=cylon_env, **kwargs)
        return ray.get(self._run_cylon_remote(fn=f))

    def run(self,
            fn: Callable[[Any], Any],
            args: Optional[List] = None,
            kwargs: Optional[Dict] = None) -> List[Any]:
        args = args or []
        kwargs = kwargs or {}
        f = lambda self_obj: fn(*args, **kwargs)
        return ray.get(self._run_remote(fn=f))

    def execute(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function to be invoked on every object.

        Returns:
            Deserialized return values from the target function.
        """
        return ray.get(self._run_remote(fn))

    def execute_cylon(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function to be invoked on every object.

        Returns:
            Deserialized return values from the target function.
        """
        return ray.get(self._run_cylon_remote(fn))

    @abstractmethod
    def shutdown(self):
        """Destroys the workers."""
        for worker in self.workers:
            del worker

        if self.placement_group:
            ray.util.remove_placement_group(self.placement_group)
            self.placement_group = None


class CylonRayFileStoreWorkerPool(CylonRayWorkerPool):
    def __init__(self, num_workers, pg_strategy='STRICT_SPREAD', pg_timeout=100, config: GlooFileStoreConfig = None):
        super().__init__(num_workers, pg_strategy, pg_timeout)
        self.gloo_file_store_path = config.file_store_path

        self.actor_cls = CylonRayFileStoreActor
        self.actor_kwargs = {
            'file_store_path': config.file_store_path,
            'store_prefix': config.store_prefix or str(ray.get_runtime_context().job_id)
        }

        os.makedirs(config.file_store_path, exist_ok=True)

    def shutdown(self):
        super().shutdown()

        if os.path.exists(self.gloo_file_store_path):
            shutil.rmtree(self.gloo_file_store_path)
