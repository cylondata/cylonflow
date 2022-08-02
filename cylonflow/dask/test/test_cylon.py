from pycylon import CylonEnv

from cylonflow.dask.executor import CylonDaskExecutor
from cylonflow.api.config import GlooFileStoreConfig


def task1(x, y=None, cylon_env: CylonEnv = None):
    cylon_env.barrier()
    return (x, y, cylon_env.rank, cylon_env.world_size) if cylon_env else None


def task2(cylon_env: CylonEnv = None):
    cylon_env.barrier()
    return (cylon_env.rank, cylon_env.world_size) if cylon_env else None


def task3():
    return 30000, 400000


class Foo:
    def __init__(self, x, y=None):
        self.x = x
        self.y = y

    def run(self, cylon_env: CylonEnv = None):
        cylon_env.barrier()
        return (self.x, self.y, cylon_env.rank, cylon_env.world_size) if cylon_env else None



config = GlooFileStoreConfig()
executor = CylonDaskExecutor(4, config, scheduler_file='/home/niranda/sched.json')
executor.start(Foo, [10000], {'y': 20000})

xx = 1000
yy = 2000
result = executor.run_cylon(task1, args=[xx], kwargs={'y': yy})
print(result)

result = executor.run_cylon(task2)
print(result)

result = executor.execute_cylon(lambda foo, cylon_env: foo.run(cylon_env=cylon_env))
print(result)

executor.shutdown()
