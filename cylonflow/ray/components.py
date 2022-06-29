import logging
from typing import Callable, Any, Optional, List, Dict

import ray

logger = logging.getLogger(__name__)


class CylonRayActor:
    """
    Actor class at the workers
    """

    def __init__(self, world_rank=0, world_size=1) -> None:
        self.executable = None
        # todo init cylon_ctx here

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


class CylonRayWorkerPool:
    """
    Acts as the remote object that dispatches tasks/ actors to workers.
    """

    def __init__(self, num_workers) -> None:
        self.num_workers = num_workers
        self.pg_strategy = 'STRICT_SPREAD'
        self.pg_timeout = 100

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
            actor = ray.remote(CylonRayActor)
            actor_with_opts = actor.options(num_cpus=1,
                                            placement_group_capture_child_tasks=False,
                                            placement_group=self.placement_group,
                                            placement_group_bundle_index=idx)
            worker = actor_with_opts.remote(world_rank=idx, world_size=self.num_workers)
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

    def start(self,
              executable_cls: type = None,
              executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None,
              extra_env_vars: Optional[Dict] = None):
        self._create_workers()

        start_futures = [
            w.start_executable.remote(executable_cls, executable_args, executable_kwargs)
            for w in self.workers]
        ray.get(start_futures)

    def run(self,
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
        f = lambda w: fn(*args, **kwargs)
        return ray.get(self._run_remote(fn=f))

    def execute(self, fn: Callable[["executable_cls"], Any],
                callbacks: Optional[List[Callable]] = None) -> List[Any]:
        """Executes the provided function on all workers.

        Args:
            fn: Target function to be invoked on every object.
            callbacks: List of callables. Each callback must either
                be a callable function or a class that implements __call__.
                Every callback will be invoked on every value logged
                by the rank 0 worker.

        Returns:
            Deserialized return values from the target function.
        """
        return ray.get(self._run_remote(fn))

    def shutdown(self):
        """Destroys the workers."""
        for worker in self.workers:
            del worker

        if self.placement_group:
            ray.util.remove_placement_group(self.placement_group)
            self.placement_group = None


class CylonRayExecutor:
    """
    Driver class
    """

    def __init__(self, num_workers):
        self.num_workers = num_workers

        self.remote_worker_pool = None

    def start(self,
              executable_cls: type = None,
              executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None,
              extra_env_vars: Optional[Dict] = None):
        self.remote_worker_pool = ray.remote(CylonRayWorkerPool).remote(self.num_workers)

    def run(self,
            fn: Callable[[Any], Any],
            args: Optional[List] = None,
            kwargs: Optional[Dict] = None) -> List[Any]:
        pass

    def execute(self, fn: Callable[["executable_cls"], Any],
                callbacks: Optional[List[Callable]] = None) -> List[Any]:
        pass
