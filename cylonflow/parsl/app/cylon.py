import inspect
from typing import Optional, Union, List, Literal

import typeguard
from parsl import DataFlowKernel, DataFlowKernelLoader
from parsl.app.python import PythonApp, timeout

from cylonflow.parsl import CYLON_COMM_KEY, CYLON_LOCAL_COMM_KEY, CYLON_ENV_KEY
from cylonflow.parsl.executor import CylonExecutor, CylonEnvExecutor


class CylonBspApp(PythonApp):
    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all',
                 ignore_for_cache=None, join=False):
        if not self._check_args(inspect.getfullargspec(func)):
            raise ValueError(f"{func.__name__} args should have either **kwargs or "
                             f"(\'comm\' and \'local_comm\')")

        super().__init__(func,
                         data_flow_kernel=data_flow_kernel,
                         executors=executors,
                         cache=cache,
                         ignore_for_cache=ignore_for_cache,
                         join=join)

    def valid_executors(self):
        return CylonExecutor

    def _check_args(self, arg_spec):
        """
        Checks if args are valid. Following args are valid
        foo(..., **kwargs)
        foo(..., comm=None, **kwargs)
        foo(..., local_comm=None, **kwargs)
        foo(..., comm=None, local_comm=None)
        """
        return arg_spec.varkw is not None or (
                CYLON_COMM_KEY in arg_spec.args and CYLON_LOCAL_COMM_KEY in arg_spec.args)

    def __call__(self, *args, **kwargs):
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        # check if all executors are cylon executors
        for label, ex in dfk.executors.items():
            if label != '_parsl_internal' and not isinstance(ex, self.valid_executors()):
                raise ValueError("CylonApp only supports CylonExecutor")

        walltime = invocation_kwargs.get('walltime')
        if walltime is not None:
            func = timeout(self.func, walltime)
        else:
            func = self.func

        app_fut = dfk.submit(func, app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs,
                             join=self.join)

        return app_fut


class CylonEnvApp(CylonBspApp):
    def _check_args(self, arg_spec):
        """
        Checks if args are valid. Following args are valid
        foo(..., **kwargs)
        foo(..., cylon_env=None)
        foo(..., cylon_env=None, **kwargs)
        """
        return arg_spec.varkw is not None or CYLON_ENV_KEY in arg_spec.args

    def valid_executors(self):
        return CylonEnvExecutor


@typeguard.typechecked
def cylon_bsp_app(function=None,
                  data_flow_kernel: Optional[DataFlowKernel] = None,
                  cache: bool = False,
                  executors: Union[List[str], Literal['all']] = 'all',
                  ignore_for_cache: Optional[List[str]] = None,
                  join: bool = False):
    """Decorator function for making python apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, ``@python_app`` if using all defaults or ``@python_app(walltime=120)``. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    join : bool
        If True, this app will be a join app: the decorated python code must return a Future
        (rather than a regular value), and and the corresponding AppFuture will complete when
        that inner future completes.
    ignore_for_cache: list
    """

    def decorator(func):
        def wrapper(f):
            return CylonBspApp(f,
                               data_flow_kernel=data_flow_kernel,
                               cache=cache,
                               executors=executors,
                               ignore_for_cache=ignore_for_cache,
                               join=join)

        return wrapper(func)

    if function is not None:
        return decorator(function)
    return decorator


@typeguard.typechecked
def cylon_env_app(function=None,
                  data_flow_kernel: Optional[DataFlowKernel] = None,
                  cache: bool = False,
                  executors: Union[List[str], Literal['all']] = 'all',
                  ignore_for_cache: Optional[List[str]] = None,
                  join: bool = False):
    """Decorator function for making python apps.

    Parameters
    ----------
    function : function
        Do not pass this keyword argument directly. This is needed in order to allow for omitted parenthesis,
        for example, ``@python_app`` if using all defaults or ``@python_app(walltime=120)``. If the
        decorator is used alone, function will be the actual function being decorated, whereas if it
        is called with arguments, function will be None. Default is None.
    data_flow_kernel : DataFlowKernel
        The :class:`~parsl.dataflow.dflow.DataFlowKernel` responsible for managing this app. This can
        be omitted only after calling :meth:`parsl.dataflow.dflow.DataFlowKernelLoader.load`. Default is None.
    executors : string or list
        Labels of the executors that this app can execute over. Default is 'all'.
    cache : bool
        Enable caching of the app call. Default is False.
    join : bool
        If True, this app will be a join app: the decorated python code must return a Future
        (rather than a regular value), and and the corresponding AppFuture will complete when
        that inner future completes.
    ignore_for_cache: list
    """

    def decorator(func):
        def wrapper(f):
            return CylonEnvApp(f,
                               data_flow_kernel=data_flow_kernel,
                               cache=cache,
                               executors=executors,
                               ignore_for_cache=ignore_for_cache,
                               join=join)

        return wrapper(func)

    if function is not None:
        return decorator(function)
    return decorator
