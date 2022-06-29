from abc import ABC, abstractmethod
from enum import IntEnum
from typing import Union, List, Any

from parsl.serialize import deserialize


class CylonDistResult:
    """
    Container for keeping serialized results from each worker
    """

    def __init__(self, data):
        """
        Parameters
        ----------
        data: list or byte buffers
            serialized data
        """
        self.data_ = data

    @property
    def size(self):
        return len(self.data_)

    def __getitem__(self, index):
        return deserialize(self.data_[index])

    def __str__(self) -> str:
        return '; '.join([str(self[i]) for i in range(self.size)])


class ResultKind(IntEnum):
    TABLE = 0
    COLUMN = 1
    SCALAR = 2
    UNKNOWN = 3
    PYOBJ = 4


class CylonRemoteResult:
    def __init__(self, tid: int, kind: int, payloads: Union[int, List[Any]], success: bool):
        """
        Container for keeping serialized results from each worker

        Parameters
        ----------
        tid: task ID
        kind: result kind (0 = Table, 1 = Column, 2 = Scalar)
        success: bool
            success/ failure
        """
        self.tid_ = tid
        self.kind_ = ResultKind(kind)
        self.payloads_ = payloads
        self.success_ = success
        self.length_ = 1 if self.kind_ == ResultKind.SCALAR else len(payloads)

    @property
    def tid(self):
        return self.tid_

    @property
    def is_ok(self):
        return self.success_

    @property
    def payloads(self):
        return self.payloads_

    @property
    def kind(self):
        return self.kind_

    def is_remote(self):
        return self.kind_ > 2  # i.e. UNKNOWN or PYOBJ

    def __len__(self):
        return self.length_

    def __repr__(self):
        return f'CylonRemoteResultImpl[tid:{self.tid_}, success:{self.success_}, ' \
               f'kind:{self.kind_}, len:{len(self)}]'
