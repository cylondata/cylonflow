import ray
from pycylon import CylonEnv

from cylonflow.ray.executor import CylonRayExecutor


def task1(x, y=None, cylon_env: CylonEnv = None):
    cylon_env.barrier()
    return (x, y, cylon_env.rank, cylon_env.world_size) if cylon_env else None


def task2(cylon_env: CylonEnv = None):
    cylon_env.barrier()
    return (cylon_env.rank, cylon_env.world_size) if cylon_env else None


ray.init(address='localhost:6379', _redis_password='1234')

executor = CylonRayExecutor(2, pg_strategy='SPREAD')
executor.start()

xx = 1000
yy = 2000
result = executor.run(task1, args=[xx], kwargs={'y': yy})
print(result)
print(ray.get(result))

result = executor.run(task2)
print(result)
print(ray.get(result))
