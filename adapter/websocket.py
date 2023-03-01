import asyncio
from typing import Callable, Dict, List, Optional

from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import Logger
from nautilus_trader.network.websocket import WebSocketClient


class AlpacaWebSocketClient(WebSocketClient):
    """
    Provides a 'Alpaca' streaming WebSocket client.
    """

    TRADING: str = "wss://paper-api.alpaca.markets/stream"

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            clock: LiveClock,
            logger: Logger,
            key: str,
            secret: str,
            msg_handler: Callable[[bytes], None],
            reconnect_handler: Callable[[], None],
            auto_ping_interval: Optional[float] = None,
            log_send: bool = False,
            log_recv: bool = False,
            subscription_plan: str = None,
            paper_trading: bool = None,
    ):
        super().__init__(
            loop=loop,
            logger=logger,
            handler=msg_handler,
            max_retry_connection=6,
            pong_msg=b'{"type":"pong"}',
            log_send=log_send,
            log_recv=log_recv,
        )

        if subscription_plan == 'iex':
            self._base_url = 'wss://stream.data.alpaca.markets/v2/iex'
        elif subscription_plan == 'sip':
            self._base_url = 'wss://stream.data.alpaca.markets/v2/sip'
        elif subscription_plan not in {'sip', 'iex', None}:
            self._log.error("Unsupported value for subscription plan.")
        else:
            if paper_trading:
                self._base_url = 'wss://paper-api.alpaca.markets/stream'
            else:
                self._base_url = "wss://api.alpaca.markets/stream"

        self._clock = clock
        self._key = key
        self._secret = secret

        self._reconnect_handler = reconnect_handler
        self._streams: List[Dict] = []

        # Tasks
        self._auto_ping_interval = auto_ping_interval
        self._task_auto_ping: Optional[asyncio.Task] = None

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def subscriptions(self):
        return self._streams.copy()

    @property
    def has_subscriptions(self):
        if self._streams:
            return True
        else:
            return False

    async def connect(self, start: bool = True, **ws_kwargs) -> None:
        await super().connect(ws_url=self._base_url, start=start, **ws_kwargs)

    async def post_connection(self):
        if self._key is None or self._secret is None:
            self._log.info("Unauthenticated session (no credentials provided).")
            return

        login = {
            "action": "auth",
            "key": self._key,
            "secret": self._secret,
        }

        await self.send_json(login)

        if self._auto_ping_interval and self._task_auto_ping is None:
            self._task_auto_ping = self._loop.create_task(self._auto_ping())

        self._log.info("Session authenticated.")

    async def post_reconnection(self):
        # Re-login and authenticate
        await self.post_connection()

        # Resubscribe to all streams
        for subscription in self._streams:
            await self.send_json({"action": "subscribe", **subscription})

        self._reconnect_handler()

    async def post_disconnection(self) -> None:
        if self._task_auto_ping is not None:
            self._task_auto_ping.cancel()
            self._task_auto_ping = None  # Clear canceled task

    async def _auto_ping(self) -> None:
        while True:
            await asyncio.sleep(self._auto_ping_interval)
            await self._ping()

    async def _ping(self) -> None:
        await self.send_json({"op": "ping"})

    async def _subscribe(self, subscription: Dict) -> None:
        if subscription not in self._streams:
            await self.send_json({"action": "subscribe", **subscription})
            self._streams.append(subscription)

    async def _unsubscribe(self, subscription: Dict) -> None:
        if subscription in self._streams:
            await self.send_json({"action": "unsubscribe", **subscription})
            self._streams.remove(subscription)

    async def subscribe_trade_updates(self) -> None:
        data = {
            "action": "listen",
            "data": {
                "streams": ["trade_updates"]
            }
        }
        await self.send_json(data)

    async def subscribe_markets(self) -> None:
        subscription = {"channel": "markets"}
        await self._subscribe(subscription)

    async def subscribe_bars(self, market: str) -> None:
        subscription = {"bars": [market]}
        await self._subscribe(subscription)

    async def subscribe_trades(self, market: str) -> None:
        subscription = {"trades": [market]}
        await self._subscribe(subscription)

    async def subscribe_quotes(self, market: str) -> None:
        subscription = {"quotes": [market]}
        await self._subscribe(subscription)

    async def unsubscribe_markets(self) -> None:
        subscription = {"channel": "markets"}
        await self._unsubscribe(subscription)

    async def unsubscribe_bars(self, market: str) -> None:
        subscription = {"bars": [market]}
        await self._unsubscribe(subscription)

    async def unsubscribe_trades(self, market: str) -> None:
        subscription = {"trades": [market]}
        await self._unsubscribe(subscription)

    async def unsubscribe_quotes(self, market: str) -> None:
        subscription = {"quotes": [market]}
        await self._unsubscribe(subscription)
