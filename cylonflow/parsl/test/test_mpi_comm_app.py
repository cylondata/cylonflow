import parsl
from parsl.config import Config
from parsl.providers import LocalProvider

from cylonflow.parsl.app.cylon import cylon_bsp_app
from cylonflow.parsl.data.result import CylonDistResult
from cylonflow.parsl.executor import CylonExecutor

host_file_txt = """localhost slots=8
"""

config = Config(
    executors=[
        CylonExecutor(
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


@cylon_bsp_app
def foo(x_, comm=None, local_comm=None, **kwargs):
    from mpi4py.MPI import SUM

    rank = comm.rank
    world_sz = comm.size

    local_rank = local_comm.rank
    local_world_sz = local_comm.size

    print(f"Starting rank: {rank} world_size: {world_sz} local_rank {local_rank} local_world_sz "
          f"{local_world_sz}")

    # import time
    # time.sleep(x + rank)  # Sleep for 2 seconds

    out = local_comm.allreduce(x_ * 4, SUM)  # all gather in the local comm

    # return f"payload:[{x_} {rank}  {world_sz} {out}]"
    return out + 100 * local_rank


@cylon_bsp_app
def bar(x_: CylonDistResult, y_: CylonDistResult, **kwargs):
    # import time
    # time.sleep(2)  # Sleep for 2 seconds
    local_rank = kwargs['local_comm'].rank

    return f"x: {x_[local_rank]}  y: {y_[local_rank]}"


x = foo(1)
y = foo(2)

xy = bar(x.result(), y.result())
print(xy.result())
