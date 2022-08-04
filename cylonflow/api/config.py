from dataclasses import dataclass


@dataclass
class GlooFileStoreConfig:
    file_store_path: str = '/tmp/gloo'
    store_prefix: str = None
    tcp_iface: str = None
    tcp_host_name: str = None
    tcp_ai_family: int = None
