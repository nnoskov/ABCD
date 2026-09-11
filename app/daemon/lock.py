import os
import sys

class SingleInstanceLock:
    def __init__(self, path: str):
        self.path = path
        self._fh = None

    def acquire(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, "a+")
        if sys.platform.startswith("win"):
            import msvcrt
            try:
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise SystemExit("daemon already running (lock busy)")
        else:
            import fcntl
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                raise SystemExit("daemon already running (lock busy)")

    def release(self) -> None:
        if not self._fh:
            return
        try:
            if sys.platform.startswith("win"):
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None