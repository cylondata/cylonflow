from dataclasses import dataclass


@dataclass
class GlooFileStoreConfig:
    file_store_path: str = '/tmp/gloo'
    store_prefix: str = None
