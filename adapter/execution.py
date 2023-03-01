import asyncio

import msgspec
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any, Set

from nautilus_trader.core.datetime import secs_to_millis

from adapter.core import ALPACA_VENUE
from adapter.error import AlpacaError
from adapter.http import AlpacaHttpClient
from adapter.websocket import AlpacaWebSocketClient
from adapter.providers import AlpacaInstrumentProvider
from adapter.parser import parse_order_status_http, parse_order_status
from adapter.parser import parse_position_report
from adapter.parser import parse_trade_report
from adapter.parser import parse_order_type

from nautilus_trader.accounting.accounts.margin import MarginAccount
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.enums import LogColor
from nautilus_trader.common.logging import Logger
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.execution.reports import TradeReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.orders.base import Order
from nautilus_trader.model.position import Position
from nautilus_trader.msgbus.bus import MessageBus

from nautilus_trader.model.enums import order_side_to_str
from nautilus_trader.model.enums import order_side_from_str
from nautilus_trader.model.enums import time_in_force_to_str
from nautilus_trader.model.enums import time_in_force_from_str

# The 'pragma: no cover' comment excludes a method from test coverage.
# https://coverage.readthedocs.io/en/coverage-4.3.3/excluding.html
# The reason for their use is to reduce redundant/needless tests which simply
# assert that a `NotImplementedError` is raised when calling abstract methods.
# These tests are expensive to maintain (as they must be kept in line with any
# refactorings), and offer little to no benefit in return. However, the intention
# is for all method implementations to be fully covered by tests.

# *** THESE PRAGMA: NO COVER COMMENTS MUST BE REMOVED IN ANY IMPLEMENTATION. ***


class AlpacaExecutionClient(LiveExecutionClient):
    """
    An example of a ``LiveExecutionClient`` highlighting the method requirements.

    +----------------------------------+-------------+
    | Method                           | Requirement |
    +----------------------------------+-------------+
    | connect                          | required - IN PROGRESS   |
    | disconnect                       | required - TEST   |
    +------------------------------------------------+
    | submit_order                     | required - IN PROGRESS   |
    | submit_order_list                | required - TO DO   |
    | modify_order                     | required - TO DO   |
    | cancel_order                     | required - TEST   |
    | cancel_all_orders                | required - IN PROGRESS   |
    | generate_order_status_report     | required - IN PROGRESS   |
    | generate_order_status_reports    | required - IN PROGRESS   |
    | generate_trade_reports           | required - IN PROGRESS   |
    | generate_position_status_reports | required - IN PROGRESS   |
    | handle_order_updates             | optional - IN PROGRESS   |
    +------------------------------------------------+
    """

    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            client: AlpacaHttpClient,
            account_id: AccountId,
            msgbus: MessageBus,
            cache: Cache,
            clock: LiveClock,
            logger: Logger,
            paper_trading: bool,
            instrument_provider: AlpacaInstrumentProvider,
    ):
        super().__init__(
            loop=loop,
            client_id=ClientId(ALPACA_VENUE.value),
            venue=ALPACA_VENUE,
            instrument_provider=instrument_provider,
            base_currency=None,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            oms_type=OmsType.NETTING,  # What is this doing?
            account_type=AccountType.MARGIN,  # What is this doing?
        )

        self._initial_leverage_set = False

        self._http_client = client
        self._ws_client = AlpacaWebSocketClient(
            key=client.api_key,
            secret=client.api_secret,
            loop=loop,
            clock=clock,
            logger=logger,
            msg_handler=self._handle_order_updates,
            reconnect_handler=self._handle_ws_reconnect,
            paper_trading=paper_trading,
        )

        self._set_account_id(account_id)

    def connect(self) -> None:
        self._log.info('Connecting...')
        self._loop.create_task(self._connect())

    def disconnect(self) -> None:
        self._log.info('Disconnecting...')
        self._loop.create_task(self._disconnect())

    async def _connect(self) -> None:
        # Connect HTTP client
        if not self._http_client.connected:
            await self._http_client.connect()
        try:
            await self._instrument_provider.initialize()
        except Exception as ex:
            self._log.exception('Error on connect', ex)
            return

        self._log.info('Alpaca API key authenticated.', LogColor.GREEN)
        self._log.info(f'API key {self._http_client.api_key}.')

        # Update account state
        await self._update_account_state()
        # self._task_poll_account = self._loop.create_task(self._poll_account_state())

        # Connect WebSocket client
        if not self._http_client.connected:
            await self._http_client.connect()
        try:
            await self._ws_client.connect()
        except Exception as e:
            self._log.exception("Error on connecting websocket", e)
            return

        self._set_connected(True)
        self._log.info('Connected.')

        await self._ws_client.subscribe_trade_updates()

    async def _disconnect(self) -> None:
        # if self._task_poll_account:
        #     self._task_poll_account.cancel()

        # Disconnect WebSocket client
        if self._ws_client.is_connected:
            await self._ws_client.disconnect()

        # Disconnect HTTP client
        if self._http_client.connected:
            await self._http_client.disconnect()

        self._set_connected(False)
        self._log.info("Disconnected.")

    # -- ACCOUNT MANAGEMENT ------------------------------------------------------------------------

    async def _update_account_state(self) -> None:
        self._log.debug("Updating account state...")

        response: Dict[str, Any] = await self._http_client.get_account_info()
        if self.account_id is None:
            self._set_account_id(AccountId(f"{ALPACA_VENUE.value}-{response['account_number']}"))

        self._handle_account_info(response)

        if not self._initial_leverage_set:
            account: Optional[MarginAccount] = self._cache.account(self.account_id)
            leverage = Decimal(response["multiplier"])
            account.set_default_leverage(leverage)
            self._log.info(
                f"Setting {self.account_id} default leverage to {leverage}X.",
                LogColor.BLUE,
            )

            self._initial_leverage_set = True

    def _handle_account_info(self, info: Dict[str, Any]) -> None:
        total = Money(info['equity'], USD)
        locked = Money(info['short_market_value'] + info['long_market_value'], USD)
        free = Money(total - locked, USD)

        balance = AccountBalance(
            total=total,
            locked=locked,
            free=free,
        )

        # TODO(cs): Uncomment for development
        # self._log.info(str(json.dumps(info, indent=4)), color=LogColor.GREEN)

        margins: List[MarginBalance] = []

        # TODO(cs): Margins on FTX are fractions - determine solution
        # for position in info["positions"]:
        #     margin = MarginBalance(
        #         initial=Money(position["initialMarginRequirement"], USD),
        #         maintenance=Money(position["maintenanceMarginRequirement"], USD),
        #         instrument_id=InstrumentId(Symbol(position["future"]), FTX_VENUE),
        #     )
        #     margins.append(margin)

        self.generate_account_state(
            balances=[balance],
            margins=margins,
            reported=True,
            ts_event=self._clock.timestamp_ns(),
            info=info,
        )

        self._log.info(
            f"initialMarginRequirement={info['initial_margin']}, "
            f"maintenanceMarginRequirement={info['maintenance_margin']}, "
            f"totalAccountValue={info['equity']}, "
            f"totalShortValue={info['short_market_value']}, "
            f"totalLongValue={info['long_market_value']} ",
            LogColor.BLUE,
        )

    # -- EXECUTION REPORTS ------------------------------------------------------------------------

    async def generate_order_status_report(
            self,
            instrument_id: InstrumentId,
            client_order_id: Optional[ClientOrderId] = None,
            venue_order_id: Optional[VenueOrderId] = None,
    ) -> Optional[OrderStatusReport]:
        """
        Generate an order status report for the given order identifier parameter(s).
        If the order is not found, or an error occurs, then logs and returns
        ``None``.
        Parameters
        ----------
        instrument_id : InstrumentId, optional
            The instrument ID query filter.
        client_order_id : ClientOrderId, optional
            The client order ID for the report.
        venue_order_id : VenueOrderId, optional
            The venue order ID (assigned by the venue) query filter.
        Returns
        -------
        OrderStatusReport or ``None``
        Raises
        ------
        ValueError
            If both the `client_order_id` and `venue_order_id` are ``None``.
        """
        PyCondition.true(
            client_order_id is not None or venue_order_id is not None,
            "both `client_order_id` and `venue_order_id` were `None`",
        )

        self._log.info(
            f"Generating OrderStatusReport for "
            f"{repr(client_order_id) if client_order_id else ''} "
            f"{repr(venue_order_id) if venue_order_id else ''}..."
        )

        if venue_order_id:
            try:
                response = await self._http_client.get_order_status(venue_order_id.value)
            except AlpacaError as ex:
                self._log.error(
                    f"Cannot get order status for {venue_order_id.value}: {ex.message}",
                )
                return None
        else:
            try:
                response = await self._http_client.get_order_status_by_client_id(
                    client_order_id.value)
            except AlpacaError as ex:
                self._log.error(
                    f"Cannot get order status for {client_order_id.value}: {ex.message}",
                )
                return None

        # Get instrument
        instrument_id = InstrumentId(Symbol(response['symbol']), ALPACA_VENUE)
        instrument = self._instrument_provider.find(instrument_id)
        if instrument is None:
            self._log.error(
                f"Cannot generate order status report: "
                f"no instrument found for {instrument_id}.",
            )
            return None

        return parse_order_status_http(
            account_id=self.account_id,
            instrument=instrument,
            data=response,
            report_id=UUID4(),
            ts_init=self._clock.timestamp_ns(),
        )

    async def generate_order_status_reports(
            self,
            instrument_id: InstrumentId = None,
            start: datetime = None,
            end: datetime = None,
            open_only: bool = False,
    ) -> List[OrderStatusReport]:
        """
         Generate a list of order status reports with optional query filters.
         The returned list may be empty if no orders match the given parameters.
         Parameters
         ----------
         instrument_id : InstrumentId, optional
             The instrument ID query filter.
         start : datetime, optional
             The start datetime query filter.
         end : datetime, optional
             The end datetime query filter.
         open_only : bool, default False
             If the query is for open orders only.
         Returns
         -------
         list[OrderStatusReport]
         """
        self._log.info(f"Generating OrderStatusReports for {self.id}...")

        open_orders = self._cache.orders_open(venue=self.venue)
        active_symbols: Set[str] = {
            o.instrument_id.symbol.value for o in open_orders
        }

        order_msgs = []
        reports: Dict[VenueOrderId, OrderStatusReport] = {}

        try:
            open_order_msgs: List[Dict[str, Any]] = await self._http_client.get_open_orders(
                market=instrument_id.symbol.value if instrument_id is not None else None,
            )
            if open_order_msgs:
                order_msgs.extend(open_order_msgs)
                # Add to active symbols
                for o in open_order_msgs:
                    active_symbols.add(o["symbol"])

            for symbol in active_symbols:
                response = await self._http_client.get_order_history(
                    market=symbol,
                    after=secs_to_millis(start.timestamp()) if start is not None else None,
                    until=secs_to_millis(end.timestamp()) if end is not None else None,
                )
                order_msgs.extend(response)
        except AlpacaError as e:
            self._log.exception(f"Cannot generate order status report: {e.message}", e)
            return []

        for msg in order_msgs:
            # Apply filter (always report open orders regardless of start, end filter)
            # TODO(cs): Time filter is WIP
            # timestamp = pd.to_datetime(data["time"], utc=True)
            # if data["status"] not in ("NEW", "PARTIALLY_FILLED", "PENDING_CANCEL"):
            #     if start is not None and timestamp < start:
            #         continue
            #     if end is not None and timestamp > end:
            #         continue

            instrument_id = InstrumentId(Symbol(msg['symbol']), ALPACA_VENUE)
            instrument = self._instrument_provider.find(instrument_id)

            report: OrderStatusReport = parse_order_status_http(
                account_id=self.account_id,
                instrument=instrument,
                data=msg,
                report_id=UUID4(),
                ts_init=self._clock.timestamp_ns(),
            )

            self._log.debug(f"Received {report}.")
            reports[report.venue_order_id] = report  # One report per order

        len_reports = len(reports)
        plural = "" if len_reports == 1 else "s"
        self._log.info(f"Generated {len(reports)} OrderStatusReport{plural}.")

        return list(reports.values())

    async def generate_trade_reports(
            self,
            instrument_id: InstrumentId = None,
            venue_order_id: VenueOrderId = None,
            start: datetime = None,
            end: datetime = None,
    ) -> List[TradeReport]:
        if instrument_id is not None:
            self._log.info(f'InstrumentId not implemented and disregarded.')
        if venue_order_id is not None:
            self._log.info(f'VenueOrderId not implemented and disregarded.')
        self._log.info(f"Generating TradeReports for {self.id}...")

        reports: List[TradeReport] = []

        try:
            fills_response: List[Dict[str, Any]] = await self._http_client.get_fills(
                after=int(start.timestamp()) if start is not None else None,
                until=int(end.timestamp()) if end is not None else None,
            )
        except AlpacaError as e:
            self._log.exception("Cannot generate trade report: ", e)
            return []

        if fills_response:
            for data in fills_response:
                # Get instrument
                instrument_id = InstrumentId(Symbol(data['symbol']), ALPACA_VENUE)
                instrument = self._instrument_provider.find(instrument_id)
                if instrument is None:
                    self._log.error(
                        f"Cannot generate trade report: "
                        f"no instrument found for {instrument_id}.",
                    )
                    continue

                report: TradeReport = parse_trade_report(
                    account_id=self.account_id,
                    instrument=instrument,
                    data=data,
                    report_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                )

                reports.append(report)

        # Sort in ascending order (adding 'order' to `get_fills()` breaks the client)
        reports = sorted(reports, key=lambda x: x.trade_id)

        len_reports = len(reports)
        plural = "" if len_reports == 1 else "s"
        self._log.info(f"Generated {len(reports)} TradeReport{plural}.")

        return reports

    async def generate_position_status_reports(
            self,
            instrument_id: InstrumentId = None,
            start: datetime = None,
            end: datetime = None,
    ) -> List[PositionStatusReport]:
        """
        Generate a list of position status reports with optional query filters.
        The returned list may be empty if no positions match the given parameters.
        Parameters
        ----------
        instrument_id : InstrumentId, optional
            The instrument ID query filter.
        start : datetime, optional
            The start datetime query filter.
        end : datetime, optional
            The end datetime query filter.
        Returns
        -------
        list[PositionStatusReport]
        """
        self._log.info(f'Generating PositionStatusReports for {self.id}...')

        reports: List[PositionStatusReport] = []

        try:
            response: List[Dict[str, Any]] = await self._http_client.get_positions()
        except AlpacaError as ex:
            self._log.exception('Cannot generate position status report: ', ex)
            return []

        if response:
            for data in response:
                # Get instrument
                instrument_id = InstrumentId(Symbol(data['symbol']), ALPACA_VENUE)
                instrument = self._instrument_provider.find(instrument_id)
                if instrument is None:
                    self._log.error(
                        f'Cannot generate position status report: '
                        f'no instrument found for {instrument_id}.',
                    )
                    continue

                report: PositionStatusReport = parse_position_report(
                    account_id=self.account_id,
                    instrument=instrument,
                    data=data,
                    report_id=UUID4(),
                    ts_init=self._clock.timestamp_ns(),
                )

                if report.quantity == 0:
                    self._log.warning(f'Flat position. See report below.')
                    continue  # Flat position
                self._log.debug(f'Received {report}.')
                reports.append(report)

        len_reports = len(reports)
        plural = '' if len_reports == 1 else 's'
        self._log.info(f'Generated {len(reports)} PositionStatusReport{plural}.')

        return reports

    # -- COMMAND HANDLERS -------------------------------------------------------------------------

    def submit_order(self, command: SubmitOrder) -> None:
        position: Optional[Position] = None
        if command.position_id is not None:
            position = self._cache.position(command.position_id)
            if position is None:
                self._log.error(
                    f"Cannot submit order {command.order}: "
                    f"position ID {command.position_id} not found.",
                )
                return

        self._loop.create_task(self._submit_order(command.order, position))

    def submit_order_list(self, command: SubmitOrderList) -> None:
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    def modify_order(self, command: ModifyOrder) -> None:
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    def cancel_order(self, command: CancelOrder) -> None:
        self._loop.create_task(self._cancel_order(command))

    def cancel_all_orders(self, command: CancelAllOrders) -> None:
        raise NotImplementedError("method must be implemented in the subclass")  # pragma: no cover

    # -- SUBMIT ORDER -------------------------------------------------------------------------

    async def _submit_order(self, order: Order, position: Optional[Position]) -> None:
        """
        :kargs market: symbol or asset ID
        :kargs size: float. Mutually exclusive with "notional".
        :kargs side: buy or sell
        :kargs order_type: market, limit, stop, stop_limit or trailing_stop
        :kargs time_in_force: day, gtc, opg, cls, ioc, fok
        :kargs limit_price: str of float
        :kargs stop_price: str of float
        :kargs client_order_id:
        :kargs extended_hours: bool. If true, order will be eligible to execute
               in premarket/afterhours.
        :kargs order_class: simple, bracket, oco or oto
        :kargs take_profit: dict with field "limit_price" e.g
               {"limit_price": "298.95"}
        :kargs stop_loss: dict with fields "stop_price" and "limit_price" e.g
               {"stop_price": "297.95", "limit_price": "298.95"}
        :kargs trail_price: str of float
        :kargs trail_percent: str of float
        """

        self._log.debug(f"Submitting {order}.")

        # Generate event here to ensure correct ordering of events
        self.generate_order_submitted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        # To be clarified: trail_percent, limit_price/trigger_price, stop_price,
        # stop_loss, take_profit, trail_value, trail_price,

        kargs = {
            'symbol': order.instrument_id.symbol.value,
            'side': order_side_to_str(order.side).lower(),
            'qty': str(order.quantity),
            'client_order_id': order.client_order_id.value,
            'time_in_force': 'day',
            'limit_price': None,
            'stop_price': None,
            'trail_price': None,
            'trail_percent': None,
            'notional': None,
        }

        if order.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP_LIMIT}:
            kargs['limit_price'] = str(order.price)

        if order.order_type in {OrderType.MARKET, OrderType.LIMIT}:
            time_in_force = time_in_force_to_str(order.time_in_force).lower()

            if time_in_force == 'gtd':
                self._log.warning(f"Time in force 'GTD' not supported by Alpaca.")
            elif time_in_force == 'at_the_open':
                kargs['time_in_force'] = 'opg'
            elif time_in_force == 'at_the_close':
                kargs['time_in_force'] = 'cls'
            else:
                kargs['time_in_force'] = time_in_force

            if order.order_type == OrderType.MARKET:
                kargs['type'] = 'market'
            elif order.order_type == OrderType.LIMIT:
                kargs['type'] = 'limit'

        elif order.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}:
            kargs.update(
                type='stop',
                trigger_price=str(order.trigger_price)
            )
            if position is not None:
                if order.is_buy and order.trigger_price < position.avg_px_open:
                    kargs['type'] = 'take_profit'  # stop_limit?
                elif order.is_sell and order.trigger_price > position.avg_px_open:
                    kargs['type'] = 'take_profit'  # stop_limit?

        elif order.order_type in {OrderType.TRAILING_STOP_MARKET, OrderType.TRAILING_STOP_LIMIT}:
            kargs.update(
                type='trailing_stop',
                trigger_price=str(order.trigger_price),
                trail_value=str(order.trailing_offset) if order.is_buy else str(
                    -order.trailing_offset),
            )

        try:
            await self._http_client.place_order(kargs)
        except AlpacaError as ex:
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=ex.message,
                ts_event=self._clock.timestamp_ns(),  # TODO(cs): Parse from response
            )
        except Exception as ex:  # Catch all exceptions for now
            self._log.exception(
                f"Error on submit {repr(order)}"
                f"{f'for {position}' if position is not None else ''}",
                ex,
            )

    # -- CANCEL ORDER -------------------------------------------------------------------------

    async def _cancel_order(self, command: CancelOrder) -> None:
        self._log.debug(f"Canceling order {command.client_order_id.value}.")

        self.generate_order_pending_cancel(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id,
            ts_event=self._clock.timestamp_ns(),
        )
        try:
            if command.venue_order_id is not None:
                await self._http_client.cancel_order(command.venue_order_id.value)
            else:
                await self._http_client.cancel_order_by_client_id(command.client_order_id.value)
        except AlpacaError as ex:
            self._log.exception(
                f"Cannot cancel order "
                f"ClientOrderId({command.client_order_id}), "
                f"VenueOrderId{command.venue_order_id}: ",
                ex,
            )

    # -- WEBSOCKET HANDLERS --------------------------------------------------------------------

    def _handle_order_updates(self, msg: bytes) -> None:
        data: Dict[str, Any] = msgspec.json.decode(msg)

        if not data['data'].get('event'):
            self._log.debug(str(data), color=LogColor.CYAN)
            return

        order_status = data['data']['event']
        order = data['data']['order']

        client_order_id = ClientOrderId(order['client_order_id'])
        instrument_id = InstrumentId(Symbol(order['symbol']), ALPACA_VENUE)
        instrument = self._instrument_provider.find(instrument_id)
        strategy_id: StrategyId = self._cache.strategy_id_for_order(client_order_id)
        venue_order_id = VenueOrderId(order['id'])

        ts_event: int = pd.to_datetime(order['created_at'], utc=True).value

        if strategy_id is None:
            self._generate_external_order_report(
                instrument,
                client_order_id,
                venue_order_id,
                order,
                ts_event,
            )
            return

        if order_status == 'new':
            self.generate_order_accepted(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=ts_event,
            )

        elif order_status == 'fill':
            self.generate_order_filled(
                strategy_id=strategy_id,
                instrument_id=instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,  # NETTING accounts

                # FIXME: What is the difference between venue_order_id and trade_id?
                trade_id=TradeId(str(data['data']['execution_id'])),

                order_side=order_side_from_str(order['side'].upper()),
                order_type=parse_order_type(order),
                last_qty=instrument.make_qty(order['qty']),

                # FIXME: Should this be price or filled_avg_price?
                last_px=instrument.make_price(data['data']['price']),

                quote_currency=instrument.quote_currency,
                commission=Money(0, USD),
                liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
                ts_event=ts_event,
            )

        elif order_status == 'canceled':
            order = self._cache.order(client_order_id)
            if order and order.status != OrderStatus.SUBMITTED:
                self.generate_order_canceled(
                    strategy_id=strategy_id,
                    instrument_id=instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=venue_order_id,
                    ts_event=ts_event,
                )

    def _generate_external_order_report(
            self,
            instrument: Instrument,
            client_order_id: ClientOrderId,
            venue_order_id: VenueOrderId,
            data: Dict[str, Any],
            ts_event: int,
    ) -> None:
        report = OrderStatusReport(
            account_id=self.account_id,
            instrument_id=instrument.id,
            client_order_id=client_order_id,
            venue_order_id=venue_order_id,
            order_side=order_side_from_str(data['side'].upper()),
            order_type=parse_order_type(data),
            time_in_force=time_in_force_from_str(data['time_in_force'].upper()),
            order_status=parse_order_status(data),
            price=instrument.make_price(data['limit_price']) if data['limit_price'] else None,
            trigger_price=instrument.make_price(data['stop_price']) if data['stop_price'] else None,
            # trigger_type=TriggerType.LAST,
            # trailing_offset=None,
            # trailing_offset_type=TrailingOffsetType.NONE,
            quantity=instrument.make_qty(data['qty']),
            filled_qty=instrument.make_qty(data['filled_qty']),
            avg_px=Decimal(data['filled_avg_price']) if data['filled_avg_price'] else None,
            report_id=UUID4(),
            ts_accepted=ts_event,
            ts_last=ts_event,
            ts_init=self._clock.timestamp_ns(),
        )

        self._send_order_status_report(report)

    def _handle_ws_reconnect(self) -> None:
        # TODO(cs): Request order book snapshot?
        pass
