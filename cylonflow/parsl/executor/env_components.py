import logging
import pickle
import sys

import dill
import pycylon as pc
from mpi4py import MPI
from parsl.app.errors import RemoteExceptionWrapper
from parsl.serialize import serialize
from pycylon import CylonEnv
from pycylon.net import MPIConfig

from cylonflow.parsl import DEFAULT_LOGGER, CYLON_ENV_KEY, CYLON_LOGGER_KEY
from cylonflow.parsl.data.result import ResultKind, CylonRemoteResultImpl
from cylonflow.parsl.executor.components import CylonManager, CylonWorker

logger = logging.getLogger(DEFAULT_LOGGER)


class LocalStore:
    def __init__(self) -> None:
        self.map = {}

    def __getitem__(self, key):
        return self.map[key]

    def __setitem__(self, key, value):
        self.map[key] = value


class CylonEnvManager(CylonManager):
    def __init__(self, comm, rank,
                 task_q_url="tcp://127.0.0.1:50097",
                 result_q_url="tcp://127.0.0.1:50098",
                 max_queue_size=10,
                 heartbeat_threshold=120,
                 heartbeat_period=30,
                 uid=None,
                 address="127.0.0.1",
                 task_bcast_port_range=(56000, 57000),
                 worker_topic=""):
        super().__init__(comm, rank, task_q_url, result_q_url, max_queue_size, heartbeat_threshold,
                         heartbeat_period, uid, address, task_bcast_port_range, worker_topic)

    def make_final_result(self, tid, results):
        # result --> (success, res_kind, length)
        success, res_kind, len_or_payload = results[0]
        if not success:
            return self.make_exception_result(tid, len_or_payload)

        if res_kind == ResultKind.SCALAR:
            remote_r = CylonRemoteResultImpl(tid, res_kind, 1, success)
        else:
            remote_r = CylonRemoteResultImpl(tid, res_kind, [len_or_payload], success)

        for s, k, l in results[1:]:
            if not s:
                return self.make_exception_result(tid, len_or_payload)

            if k != res_kind:
                remote_r.success_ = False

            if res_kind != ResultKind.SCALAR:
                remote_r.payloads_.append(l)

        result_package = {'task_id': tid, 'result': serialize(remote_r)}
        return pickle.dumps(result_package)


class CylonEnvWorker(CylonWorker):
    def __init__(self, worker_topic, comm: MPI.Comm, local_comm: MPI.Comm):
        super().__init__(worker_topic, comm, local_comm)

        self.env = CylonEnv(config=MPIConfig(self.local_comm), distributed=local_comm.size > 1)
        self.store = LocalStore()

        logger.info(f"rank {self.env.rank} sz {self.env.world_size}")

    def make_result_package(self, req, result):
        """
        store result in the local store, and send length
        """
        tid = req['task_id']
        success = True
        res_kind = ResultKind.UNKNOWN
        if isinstance(result, pc.DataFrame):
            self.store[tid] = result.to_table().to_arrow()
            payload = len(result)
            res_kind = ResultKind.TABLE
        elif isinstance(result, pc.Column):
            self.store[tid] = result.data
            payload = len(result)
            res_kind = ResultKind.COLUMN
        elif isinstance(result, pc.Scalar):
            res_kind = ResultKind.SCALAR
            self.store[tid] = result.data
            payload = 1
        elif isinstance(result, RemoteExceptionWrapper):
            self.store[tid] = None
            success = False
            payload = result
            logger.error(f"error: {dill.loads(result.e_value)}")
        else:
            res_kind = ResultKind.PYOBJ
            self.store[tid] = None
            payload = result

        return success, res_kind, payload

    def make_exception_package(self, e):
        return False, RemoteExceptionWrapper(*sys.exc_info())

    def update_kwargs(self, kwargs):
        kwargs[CYLON_ENV_KEY] = self.env
        kwargs[CYLON_LOGGER_KEY] = logger
