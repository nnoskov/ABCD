from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set

from asyncua import Client

log = logging.getLogger(__name__)

Callback = Callable[..., None]


class _SubHandler:
    """asyncua subscription handler:
    datachange_notification(node, val, data)
    """

    def __init__(self, cb_by_node: DefaultDict[str, List[Callback]]):
        self._cb_by_node = cb_by_node

    def datachange_notification(self, node, val, data) -> None:  # noqa: N802
        nid = str(getattr(node, "nodeid", node))
        cbs = list(self._cb_by_node.get(nid, []))
        for cb in cbs:
            try:
                # support cb(node_id, value) and cb(value)
                try:
                    cb(nid, val)
                except TypeError:
                    cb(val)
            except Exception as e:
                log.debug("subscription callback failed (%s): %r", nid, e)


class AsyncuaService:
    """Thin asyncua wrapper (SecurityPolicy=None expected).

    Key points:
      - optional client_factory for tests
      - node cache
      - read_value(default=...) disconnects on failure
      - reconnect backoff
      - optional subscription support
    """

    def __init__(
        self,
        *,
        endpoint: str,
        name: str = "opcua",
        timeout_sec: float = 2.0,
        sub_interval_ms: int = 200,
        reconnect_backoff_sec: float = 1.0,
        reconnect_backoff_max_sec: float = 10.0,
        enable_subscription: bool = True,
        client_factory: Callable[..., Any] = Client,
        now: Callable[[], float] = time.monotonic,
    ):
        self.endpoint = endpoint
        self.name = name
        self.timeout_sec = float(timeout_sec)
        self.sub_interval_ms = int(sub_interval_ms)
        self.reconnect_backoff_sec = float(reconnect_backoff_sec)
        self.reconnect_backoff_max_sec = float(reconnect_backoff_max_sec)
        self.enable_subscription = bool(enable_subscription)

        self._client_factory = client_factory
        self._now = now

        self._client: Optional[Any] = None
        self._sub: Optional[Any] = None

        self._cb_by_node: DefaultDict[str, List[Callback]] = defaultdict(list)
        self._subscribed_node_ids: Set[str] = set()

        self._node_cache: Dict[str, Any] = {}

        self._last_connect_attempt = 0.0
        self._backoff = float(self.reconnect_backoff_sec)
        self._last_error: Optional[str] = None

        self._lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._client is not None

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    async def connect(self) -> None:
        async with self._lock:
            if self._client:
                return
    
            ep = self.endpoint
            if isinstance(ep, (bytes, bytearray)):
                ep = ep.decode("utf-8", errors="strict")
            else:
                ep = str(ep)
    
            c = None
            try:
                c = self._client_factory(url=ep, timeout=float(self.timeout_sec))
                await c.connect()
                self._client = c
    
                if self.enable_subscription:
                    handler = _SubHandler(self._cb_by_node)
                    self._sub = await c.create_subscription(int(self.sub_interval_ms), handler)
    
                    self._subscribed_node_ids.clear()
                    for nid in list(self._cb_by_node.keys()):
                        await self._subscribe_node(nid)
    
                self._last_error = None
                self._backoff = float(self.reconnect_backoff_sec)
                log.info("[%s] connected: %s", self.name, self.endpoint)
    
            except Exception as e:
                self._last_error = repr(e)
                self._client = None
                self._sub = None
                self._subscribed_node_ids.clear()
                self._node_cache.clear()
    
                if c is not None:
                    try:
                        await c.disconnect()
                    except Exception:
                        pass
    
                raise


    async def disconnect(self) -> None:
        async with self._lock:
            c = self._client
            sub = self._sub
            self._client = None
            self._sub = None
            self._subscribed_node_ids.clear()
            self._node_cache.clear()
            
            # allow immediate reconnect on next call (tests + real-life link drop)
            self._last_connect_attempt = 0.0
            self._backoff = float(self.reconnect_backoff_sec)

        if sub:
            try:
                await sub.delete()
            except Exception as e:
                log.debug("[%s] subscription delete failed: %r", self.name, e)
        if c:
            try:
                await c.disconnect()
            except Exception as e:
                log.debug("[%s] client disconnect failed: %r", self.name, e)


    async def ensure_connected(self) -> bool:
        if self.connected:
            return True

        now = float(self._now())
        if (now - self._last_connect_attempt) < self._backoff:
            return False

        self._last_connect_attempt = now
        try:
            await self.connect()
            return True
        except Exception as e:
            self._last_error = repr(e)
            log.warning("[%s] connect failed: %r", self.name, e)
            self._backoff = min(self._backoff * 2.0, float(self.reconnect_backoff_max_sec))
            return False


    async def reconnect(self) -> bool:
        try:
            await self.disconnect()
        except Exception:
            pass
        return await self.ensure_connected()

    # -------------------------
    # node cache
    # -------------------------
    def node(self, node_id: str) -> Any:
        nid = str(node_id)
        n = self._node_cache.get(nid)
        if n is not None:
            return n
        if not self._client:
            raise RuntimeError(f"AsyncuaService[{self.name}] is not connected")
        n = self._client.get_node(nid)
        self._node_cache[nid] = n
        return n

    # -------------------------
    # read/write helpers
    # -------------------------
    async def read_value(self, node_id: str, *, default: Any = None) -> Any:
        """Read with default; on failure -> disconnect and return default."""
        if not await self.ensure_connected():
            return default
        try:
            n = self.node(node_id)
            return await n.read_value()
        except Exception as e:
            # важно для тестов: read упал -> service disconnects
            self._last_error = repr(e)
            await self.disconnect()
            return default

    async def write_value(self, node_id: str, value: Any) -> bool:
        """Write; on failure -> disconnect and return False."""
        if not await self.ensure_connected():
            return False
        try:
            n = self.node(node_id)
            await n.write_value(value)
            return True
        except Exception as e:
            self._last_error = repr(e)
            await self.disconnect()
            return False

    async def call_method(self, object_node_id: str, method_node_id: str, *args: Any) -> Any:
        """
        Вызов OPC UA метода:
          await svc.call_method(obj_node_id, method_node_id, arg1, arg2, ...)
        """
        ok = await self.ensure_connected()
        if not ok:
            raise RuntimeError(f"AsyncuaService[{self.name}] is not connected")
        if not self._client:
            raise RuntimeError(f"AsyncuaService[{self.name}] client is None")

        obj = self._client.get_node(str(object_node_id))
        m = self._client.get_node(str(method_node_id))
        try:
            return await obj.call_method(m, *args)
        except Exception as e:
            print(e)
            # на обрывах/ошибках лучше сбросить соединение, чтобы следующий вызов переподключился
            await self.disconnect()
            raise

    # Back-compat names used by IO layers
    async def read(self, node_id: str) -> Any:
        ok = await self.ensure_connected()
        if not ok:
            self._last_error = f"AsyncuaService[{self.name}] is not connected"
            raise RuntimeError(self._last_error)
    
        try:
            n = self.node(node_id)
            return await n.read_value()
        except Exception as e:
            self._last_error = repr(e)
            await self.disconnect()
            raise
    
    
    async def write(self, node_id: str, value: Any) -> None:
        ok = await self.ensure_connected()
        if not ok:
            self._last_error = f"AsyncuaService[{self.name}] is not connected"
            raise RuntimeError(self._last_error)
    
        try:
            n = self.node(node_id)
            await n.write_value(value)
        except Exception as e:
            self._last_error = repr(e)
            await self.disconnect()
            raise

    # -------------------------
    # subscriptions
    # -------------------------
    async def subscribe_data_change(self, node_id: str, cb: Callback) -> None:
        nid = str(node_id)
        self._cb_by_node[nid].append(cb)
        if self.connected and self.enable_subscription:
            await self._subscribe_node(nid)

    async def subscribe_datachange(self, node_id: str, cb: Callback) -> None:
        # alias
        await self.subscribe_data_change(node_id, cb)

    async def _subscribe_node(self, nid: str) -> None:
        if nid in self._subscribed_node_ids:
            return
        if not self._client or not self._sub:
            return

        node = self._client.get_node(nid)
        fn = getattr(self._sub, "subscribe_data_change", None) or getattr(self._sub, "subscribe_datachange", None)
        if not fn:
            raise RuntimeError("Subscription object has no subscribe_data_change method")

        await fn(node)
        self._subscribed_node_ids.add(nid)
