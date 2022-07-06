import ray
from pycylon import CylonEnv

from cylonflow.ray.executor import CylonRayExecutor
from cylonflow.ray.worker.config import GlooFileStoreConfig


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


ray.init(address='localhost:6379', _redis_password='1234')

config = GlooFileStoreConfig()
executor = CylonRayExecutor(4, config, pg_strategy='SPREAD')
executor.start(Foo, [10000], {'y': 20000})

xx = 1000
yy = 2000
result = executor.run_cylon(task1, args=[xx], kwargs={'y': yy})
print(ray.get(result))

result = executor.run_cylon(task2)
print(ray.get(result))

result = executor.run(task3)
print(ray.get(result))

result = executor.execute_cylon(lambda foo, cylon_env: foo.run(cylon_env=cylon_env))
print(ray.get(result))

executor.shutdown()
