from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Callable, Any


class WorkerPool(ABC):
    @abstractmethod
    def run_cylon(self,
                  fn: Callable[[Any], Any],
                  args: Optional[List] = None,
                  kwargs: Optional[Dict] = None) -> List[Any]:
        """Executes the provided function on all workers.
        Note: Function signature should contain a `CylonEnv` kwarg
        ```
        def foo(cylon_env: CylonEnv = None):
            pass

        worker_pool.run_cylon(foo)
        ```
        Args:
            fn: Target function that can be executed with arbitrary
                args and keyword arguments.
            args: List of arguments to be passed into the target function.
            kwargs: Dictionary of keyword arguments to be
                passed into the target function.

        Returns:
            Deserialized return values from the target function.
        """
        raise NotImplementedError()

    @abstractmethod
    def start(self,
              executable_cls: type = None,
              executable_args: Optional[List] = None,
              executable_kwargs: Optional[Dict] = None) -> None:
        """Instantiates a class in every worker
        ```
        class Foo:
            def run(self, cylon_env: CylonEnv = None):
                pass
        worker_pool.start(Foo, ...)
        ```
        Args:
            executable_cls: Executable class
            executable_args: Executable args
            executable_kwargs: Executable kwargs

        Returns:
            None
        """
        raise NotImplementedError()

    @abstractmethod
    def execute_cylon(self, fn: Callable[["executable_cls"], Any]) -> List[Any]:
        """Executes the provided function that accepts a `CylonEnv` on all workers
        ```
        ...
        worker_pool.start(Foo, ...)
        worker_pool.execute_cylon(lambda foo, cylon_env: foo.run(cylon_env=cylon_env))
        ```

        Args:
            fn: Target function

        Returns:
            Deserialized return values from the target function.
        """
        raise NotImplementedError()

    @abstractmethod
    def shutdown(self) -> None:
        """Shuts down the worker pool

        Returns:
            None
        """
        raise NotImplementedError()
