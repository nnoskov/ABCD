import os
from app.daemon.rtk_mock import MockRTK
from app.daemon.rtk_http import HttpRTK

def make_rtk():
    backend = os.getenv("RTK_BACKEND", "mock").lower()
    
    if backend == "mock":
        return MockRTK()
    
    if backend == "http":
        base = os.environ["RTK_BASE_URL"]
        prefix = os.getenv("RTK_PREFIX", "api/master")
        timeout = float(os.getenv("RTK_TIMEOUT_SEC", "2.0"))
        return HttpRTK(base_url=base, prefix=prefix, timeout_sec=timeout)
    
    raise ValueError(f"unknown RTK_BACKEND={backend}")
