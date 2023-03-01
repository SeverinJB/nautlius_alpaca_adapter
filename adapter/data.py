from typing import List

import msgspec
import pandas as pd
import asyncio
from typing import Optional, Dict, Any

from nautilus_trader.common.enums import LogColor
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.data.tick import TradeTick, QuoteTick
from nautilus_trader.model.instruments.base import Instrument

from adapter.core import ALPACA_VENUE
from adapter.http import AlpacaHttpClient
from adapter.parser import parse_trade_ticks, parse_bar_http, parse_quote_ticks
from adapter.websocket import AlpacaWebSocketClient
from adapter.providers import AlpacaInstrumentProvider

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.base import DataType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.msgbus.bus import MessageBus
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import Logger
from nautilus_trader.cache.cache import Cache

# For testing
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.data.bar import BarSpecification
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import PriceType

from nautilus_trader.model.enums import bar_aggregation_to_str

# The 'pragma: no cover' comment excludes a method from test coverage.
# https://coverage.readthedocs.io/en/coverage-4.3.3/excluding.html
# The reason for their use is to reduce redundant/needless tests which simply
# assert that a `NotImplementedError` is raised when calling abstract methods.
# These tests are expensive to maintain (as they must be kept in line with any
# refactorings), and offer little to no benefit in return. However, the intention
# is for all method implementations to be fully covered by tests.


class AlpacaDataClient(LiveMarketDataClient):
    """
    An example of a ``LiveMarketDataClient`` highlighting the overridable abstract methods.

    A live market data client general handles market data feeds and requests.

    """

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            client: AlpacaHttpClient,
            msgbus: MessageBus,
            cache: Cache,
            clock: LiveClock,
            logger: Logger,
            subscription_plan: str,
            instrument_provider: AlpacaInstrumentProvider,
    ):
        super().__init__(
            loop=loop,
            client_id=ClientId(ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
        )

        self._http_client = client
        self._ws_client = AlpacaWebSocketClient(
            key=client.api_key,
            secret=client.api_secret,
            loop=loop,
            clock=clock,
            logger=logger,
            msg_handler=self._handle_msg,
            reconnect_handler=self._handle_ws_reconnect,
            subscription_plan=subscription_plan,
        )

    def connect(self) -> None:
        self._log.info("Connecting...")
        self._loop.create_task(self._connect())

    def disconnect(self) -> None:
        self._log.info("Disconnecting...")
        self._loop.create_task(self._disconnect())

    async def _connect(self) -> None:
        if not self._http_client.connected:
            await self._http_client.connect()
        try:
            await self._instrument_provider.initialize()
        except Exception as e:
            self._log.exception("Error on connect", e)
            return

        self._send_all_instruments_to_data_engine()

        # Connect WebSocket client
        if not self._http_client.connected:
            await self._http_client.connect()
        try:
            await self._ws_client.connect()
        except Exception as e:
            self._log.exception("Error on connecting websocket", e)
            return

        self._set_connected(True)
        self._log.info("Connected.")

    async def _disconnect(self) -> None:
        # Disconnect WebSocket client
        if self._ws_client.is_connected:
            await self._ws_client.disconnect()

        # Disconnect HTTP client
        if self._http_client.connected:
            await self._http_client.disconnect()

        self._set_connected(False)
        self._log.info("Disconnected.")

    # -- SUBSCRIPTIONS ----------------------------------------------------------------------------

    def subscribe(self, data_type: DataType) -> None:
        self._log.error(f"Cannot subscribe to {data_type.type} (not implemented).")

    def subscribe_instruments(self) -> None:  # What does this do?
        for instrument_id in list(self._instrument_provider.get_all().keys()):
            self._add_subscription_instrument(instrument_id)

    def subscribe_instrument(self, instrument_id: InstrumentId) -> None:  # What does this do?
        self._add_subscription_instrument(instrument_id)

    def subscribe_order_book_deltas(
            self,
            instrument_id: InstrumentId,
            book_type: BookType,
            depth: Optional[int] = None,
            kwargs: dict = None,
    ) -> None:
        self._log.error(f"Alpaca has order book data for crypto only (not implemented).")

    def subscribe_order_book_snapshots(
            self,
            instrument_id: InstrumentId,
            book_type: BookType,
            depth: Optional[int] = None,
            kwargs: dict = None,
    ) -> None:
        self._log.error(f"Alpaca has order book data for crypto only (not implemented).")

    def subscribe_ticker(self, instrument_id: InstrumentId) -> None:
        self._log.error(
            "Cannot subscribe to ticker data (not supported by Alpaca)."
        )

    def subscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        self._loop.create_task(self._ws_client.subscribe_quotes(instrument_id.symbol.value))

    def subscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        self._loop.create_task(self._ws_client.subscribe_trades(instrument_id.symbol.value))

    def subscribe_bars(self, bar_type: BarType) -> None:
        symbol = bar_type.instrument_id.symbol.value
        self._loop.create_task(self._ws_client.subscribe_bars(symbol))

    def subscribe_instrument_status_updates(self, instrument_id: InstrumentId) -> None:
        self._log.error(
            f"Cannot subscribe to instrument status updates for {instrument_id} "
            f"(not yet supported by NautilusTrader).",
        )

    def subscribe_instrument_close_prices(self, instrument_id: InstrumentId) -> None:
        self._log.error(
            "Cannot subscribe to instrument close prices (not supported by Alpaca)."
        )

    def unsubscribe(self, data_type: DataType) -> None:
        self._log.error(f"Cannot unsubscribe to {data_type.type} (not implemented).")

    def unsubscribe_instruments(self) -> None:
        for instrument_id in list(self._instrument_provider.get_all().keys()):
            self._remove_subscription_instrument(instrument_id)

    def unsubscribe_instrument(self, instrument_id: InstrumentId) -> None:
        self._remove_subscription_instrument(instrument_id)

    def unsubscribe_order_book_deltas(self, instrument_id: InstrumentId) -> None:
        self._log.error(f"Alpaca has order book data for crypto only (not implemented).")

    def unsubscribe_order_book_snapshots(self, instrument_id: InstrumentId) -> None:
        self._log.error(f"Alpaca has order book data for crypto only (not implemented).")

    def unsubscribe_ticker(self, instrument_id: InstrumentId) -> None:
        self._log.error(
            "Cannot unsubscribe from ticker data (not supported by Alpaca)."
        )

    def unsubscribe_quote_ticks(self, instrument_id: InstrumentId) -> None:
        self._loop.create_task(self._ws_client.unsubscribe_quotes(instrument_id.symbol.value))

    def unsubscribe_trade_ticks(self, instrument_id: InstrumentId) -> None:
        self._loop.create_task(self._ws_client.unsubscribe_trades(instrument_id.symbol.value))

    def unsubscribe_bars(self, bar_type: BarType) -> None:
        symbol = bar_type.instrument_id.symbol.value
        self._loop.create_task(self._ws_client.unsubscribe_bars(symbol))

    def unsubscribe_instrument_status_updates(self, instrument_id: InstrumentId) -> None:
        self._log.error(
            f"Cannot unsubscribe from instrument status updates for {instrument_id} "
            f"(not yet supported by NautilusTrader).",
        )

    def unsubscribe_instrument_close_prices(self, instrument_id: InstrumentId) -> None:
        self._log.error(
            "Cannot unsubscribe from instrument close prices (not supported by Alpaca)."
        )

    # -- REQUESTS ---------------------------------------------------------------------------------

    def request_instrument(self, instrument_id: InstrumentId, correlation_id: UUID4):
        instrument: Optional[Instrument] = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find instrument for {instrument_id}.")
            return

        data_type = DataType(
            type=Instrument,
            metadata={"instrument_id": instrument_id},
        )

        self._handle_data_response(
            data_type=data_type,
            data=instrument,  # Data engine handles lists of instruments
            correlation_id=correlation_id,
        )


    def request_quote_ticks(
            self,
            instrument_id: InstrumentId,
            limit: int,
            correlation_id: UUID4,
            from_datetime: Optional[pd.Timestamp] = None,
            to_datetime: Optional[pd.Timestamp] = None,
    ) -> None:
        if limit == 0 or limit > 10000:
            limit = 1000

        self._loop.create_task(
            self._request_quote_ticks(
                instrument_id,
                limit,
                correlation_id,
                from_datetime,
                to_datetime,
            )
        )

    async def _request_quote_ticks(
            self,
            instrument_id: InstrumentId,
            limit: int,
            correlation_id: UUID4,
            from_datetime: Optional[pd.Timestamp] = None,
            to_datetime: Optional[pd.Timestamp] = None,
    ) -> None:
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(
                f'Cannot parse trades response: no instrument found for {instrument_id}.',
            )
            return

        data = await self._http_client.get_quote_ticks(instrument_id.symbol.value,
                                                       from_datetime, to_datetime, limit)

        # Limit quotes data
        if limit:
            while len(data) > limit:
                data.pop(0)  # Pop left

        quote_ticks: List[QuoteTick] = parse_quote_ticks(
            instrument=instrument,
            data=data,
            ts_init=self._clock.timestamp_ns(),
        )

        self._handle_quote_ticks(instrument_id, quote_ticks, correlation_id)

    def request_trade_ticks(
            self,
            instrument_id: InstrumentId,
            limit: int,
            correlation_id: UUID4,
            from_datetime: Optional[pd.Timestamp] = None,
            to_datetime: Optional[pd.Timestamp] = None,
    ) -> None:
        self._loop.create_task(
            self._request_trade_ticks(
                instrument_id,
                limit,
                correlation_id,
                from_datetime,
                to_datetime,
            )
        )

    async def _request_trade_ticks(
            self,
            instrument_id: InstrumentId,
            limit: int,
            correlation_id: UUID4,
            from_datetime: Optional[pd.Timestamp] = None,
            to_datetime: Optional[pd.Timestamp] = None,
    ) -> None:
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(
                f'Cannot parse trades response: no instrument found for {instrument_id}.',
            )
            return

        data = await self._http_client.get_trades(instrument_id.symbol.value)

        # Limit trades data
        if limit:
            while len(data) > limit:
                data.pop(0)  # Pop left

        ticks: List[TradeTick] = parse_trade_ticks(
            instrument=instrument,
            data=data,
            ts_init=self._clock.timestamp_ns(),
        )

        data_type = DataType(
            type=TradeTick,
            metadata={
                'instrument_id': instrument_id,
                'from_datetime': from_datetime,
                'to_datetime': to_datetime,
            },
        )

        self._handle_data_response(
            data_type=data_type,
            data=ticks,
            correlation_id=correlation_id,
        )

    def request_bars(
            self,
            bar_type: BarType,
            limit: int,
            correlation_id: UUID4,
            from_datetime: Optional[pd.Timestamp] = None,
            to_datetime: Optional[pd.Timestamp] = None,
    ) -> None:
        if bar_type.is_internally_aggregated():
            self._log.error(
                f"Cannot request {bar_type}: "
                f"only historical bars with EXTERNAL aggregation available from Alpaca.",
            )
            return

        if not bar_type.spec.is_time_aggregated():
            self._log.error(
                f"Cannot request {bar_type}: only time bars are aggregated by Alpaca.",
            )
            return

        supported_aggregation = {BarAggregation.MINUTE, BarAggregation.HOUR,
                                 BarAggregation.DAY, BarAggregation.WEEK,
                                 BarAggregation.MONTH}

        if bar_type.spec.aggregation not in supported_aggregation:
            self._log.error(
                f"Cannot request {bar_type}: "
                f"{bar_aggregation_to_str(bar_type.spec.aggregation)} "
                f"bars are not aggregated by Alpaca.",
            )
            return

        if bar_type.spec.price_type != PriceType.LAST:
            self._log.error(
                f"Cannot request {bar_type}: "
                f"only historical bars for LAST price type available from Binance.",
            )
            return

        self._loop.create_task(
            self._request_bars(
                bar_type=bar_type,
                limit=limit,
                correlation_id=correlation_id,
                from_datetime=from_datetime,
                to_datetime=to_datetime,
            )
        )

    async def _request_bars(
            self,
            bar_type: BarType,
            limit: int,
            correlation_id: UUID4,
            from_datetime: Optional[pd.Timestamp] = None,
            to_datetime: Optional[pd.Timestamp] = None,
    ) -> None:
        if limit == 0 or limit > 10000:
            limit = 1000

        if bar_type.spec.aggregation == BarAggregation.MINUTE:
            resolution = "Min"
        elif bar_type.spec.aggregation == BarAggregation.HOUR:
            resolution = "Hour"
        elif bar_type.spec.aggregation == BarAggregation.DAY:
            resolution = "Day"
        elif bar_type.spec.aggregation == BarAggregation.WEEK:
            resolution = "Week"
        elif bar_type.spec.aggregation == BarAggregation.MONTH:
            resolution = "Month"

        data: List[Dict[str, Any]] = await self._http_client.get_bars(
            market=bar_type.instrument_id.symbol.value,
            timeframe=f"{bar_type.spec.step}{resolution}",
            start=from_datetime,
            end=to_datetime,
            limit=limit,
        )

        bars: list[Bar] = [
            parse_bar_http(
                bar_type,
                data=bar,
                ts_init=self._clock.timestamp_ns(),
            )
            for bar in data
        ]
        partial: Bar = bars.pop()

        self._handle_bars(bar_type, bars, partial, correlation_id)

    # -- UTILS ---------------------------------------------------------------------------------

    def _handle_msg(self, msg: bytes) -> None:
        data: List[Dict[str, Any]] = msgspec.json.decode(msg)

        for item in data:
            if item['T'] == 'b':
                self._handle_minute_bar(item)
            elif item['T'] == 't':
                self._handle_trade(item)
            elif item['T'] == 'q':
                self._handle_quote(item)
            else:
                self._log.debug(str(item), color=LogColor.CYAN)

    def _handle_minute_bar(self, bar: Dict[str, Any]) -> None:
        instrument_id = InstrumentId(Symbol(bar['S']), self.venue)
        instrument = self._instrument_provider.find(instrument_id)
        ts_init = self._clock.timestamp_ns()
        ts_event = min(dt_to_unix_nanos(bar['t']), ts_init)

        bar_spec = BarSpecification(
            step=1,
            aggregation=BarAggregation.MINUTE,
            price_type=PriceType.LAST,
        )

        bar_type = BarType(
            instrument_id=instrument_id,
            bar_spec=bar_spec,
            aggregation_source=AggregationSource.EXTERNAL,
        )

        data = Bar(
            bar_type=bar_type,
            open=Price(bar['o'], instrument.price_precision),
            high=Price(bar['h'], instrument.price_precision),
            low=Price(bar['l'], instrument.price_precision),
            close=Price(bar['c'], instrument.price_precision),
            volume=Quantity(bar['v'], instrument.size_precision),
            ts_event=ts_event,
            ts_init=ts_init
        )

        self._handle_data(data)

    def _handle_trade(self, trade: Dict[str, Any]) -> None:
        instrument_id = InstrumentId(Symbol(trade['S']), self.venue)
        instrument = self._instrument_provider.find(instrument_id)
        ts_init = self._clock.timestamp_ns()
        ts_event = min(dt_to_unix_nanos(trade['t']), ts_init)

        data = TradeTick(
            instrument_id=instrument.id,
            price=instrument.make_price(trade['p']),
            size=instrument.make_qty(trade['s']),
            aggressor_side=AggressorSide.NO_AGGRESSOR,  # Looks like Side not in Alpaca data
            trade_id=TradeId(str(trade['i'])),
            ts_event=ts_event,
            ts_init=ts_init,
        )

        self._handle_data(data)

    def _handle_quote(self, quote: Dict[str, Any]) -> None:
        instrument_id = InstrumentId(Symbol(quote['S']), self.venue)
        instrument = self._instrument_provider.find(instrument_id)
        ts_init = self._clock.timestamp_ns()
        ts_event = min(dt_to_unix_nanos(quote['t']), ts_init)

        data = QuoteTick(
            instrument_id=instrument.id,
            bid=Price(quote["bp"], instrument.price_precision),
            ask=Price(quote["ap"], instrument.price_precision),
            bid_size=Quantity(quote["bs"], instrument.size_precision),
            ask_size=Quantity(quote["as"], instrument.size_precision),
            ts_event=ts_event,
            ts_init=ts_init,
        )

        self._handle_data(data)

    def _handle_ws_reconnect(self) -> None:
        # TODO(cs): Request order book snapshot?
        pass

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)

        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)
