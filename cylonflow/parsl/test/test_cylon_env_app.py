import parsl
from parsl.config import Config
from parsl.providers import LocalProvider

from cylonflow.parsl.app.cylon import cylon_env_app
from cylonflow.parsl.data.result import CylonRemoteResult
from cylonflow.parsl.executor import CylonEnvExecutor

host_file_txt = """localhost slots=8
"""

config = Config(
    executors=[
        CylonEnvExecutor(
            label="cylon_test",
            address="127.0.0.1",
            ranks_per_node=7,
            worker_debug=True,
            heartbeat_threshold=10,
            # hostfile="nodes.txt"
            hostfile=host_file_txt,
            mpi_params="--oversubscribe",
            provider=LocalProvider(worker_init="echo AAAAAAAAAAAAA"),
        )
    ],
)

parsl.load(config=config)


@cylon_env_app
def foo(x_, cylon_env=None, logger=None):
    from pycylon import Scalar
    import pyarrow as pa

    rank = cylon_env.rank
    sz = cylon_env.world_size
    logger.info(f"Starting rank: {rank} world_size: {sz}")
    return Scalar(pa.scalar(rank + x_))


@cylon_env_app
def bar(x_: CylonRemoteResult, y_: CylonRemoteResult, logger=None, **kwargs):
    # import time
    # time.sleep(2)  # Sleep for 2 seconds
    rank = kwargs['cylon_env'].rank
    logger.info(f"rank {rank} x {x_} y {y_}")

    return f"[{rank} x: {x_}  y: {y_}]"


x = foo(100)
# res: CylonDistResult = x.result()
# print(res.is_ok, res.size, res[0], res[1])

y = foo(2)

xy = bar(x.result(), y.result())
print(xy.result())
