import os
from app.daemon.rtk_mock import MockRTK
from app.daemon.rtk_http import HttpRTK
import logging

logging.basicConfig(level=logging.DEBUG)


def make_rtk():
    backend = os.getenv("RTK_BACKEND", "http").lower()
    if backend == "mock":
        logging.debug("Creating MockRTK instance")
        return MockRTK()
    if backend == "http":
        base = os.environ["RTK_BASE_URL"]
        prefix = os.getenv("RTK_PREFIX", "api/master")
        timeout = float(os.getenv("RTK_TIMEOUT_SEC", "2.0"))
        logging.debug(
            "Creating HttpRTK instance with base_url={}, prefix={}, timeout_sec={}".format(
                base, prefix, timeout
            )
        )
        return HttpRTK(base_url=base, prefix=prefix, timeout_sec=timeout)
    raise ValueError(f"unknown RTK_BACKEND={backend}")
