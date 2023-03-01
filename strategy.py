from datetime import datetime

from numpy import double
from typing import List, Dict

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.message import Event
from nautilus_trader.indicators.average.sma import SimpleMovingAverage
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarSpecification
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events.position import PositionChanged
from nautilus_trader.model.events.position import PositionOpened
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.objects import Price
from nautilus_trader.model.orders.limit import LimitOrder
from nautilus_trader.model.position import Position
from nautilus_trader.trading.strategy import Strategy

# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class ScalpingStrategyConfig(StrategyConfig):
    """
    Configuration for 'ScalpingStrategy' instances.
    """

    instrument_ids: List[str]
    budget: int


class ScalpingStrategy(Strategy):
    """
    A strategy inspired by the Alpaca example scalping strategy (GitHub: alpacahq/example-scalping).

    Parameters
    ----------
    config : ScalpingStrategyConfig
        The configuration for the instance.
    """

    def __init__(self, config: ScalpingStrategyConfig):
        super().__init__(config)
        self.budget: int = int(self.config.budget)
        self.bar_types: List[BarType] = []
        self.instruments: List[Instrument] = []
        self.instrument_ids = [InstrumentId.from_str(i) for i in self.config.instrument_ids]

        self.sma: Dict[InstrumentId, SimpleMovingAverage] = {}
        self.sma_values: Dict[InstrumentId, List] = {}

    def on_start(self):
        """Actions to be performed on strategy start."""
        self.instruments = [self.cache.instrument(i) for i in self.instrument_ids]
        if not self.instruments:
            self.log.error(f"Could not find any instrument for {self.instrument_ids}")
            self.stop()
            return

        for instrument_id in self.instrument_ids:
            bar_type: BarType = BarType(
                instrument_id=instrument_id,
                bar_spec=BarSpecification(
                    step=1,
                    aggregation=BarAggregation.MINUTE,
                    price_type=PriceType.LAST
                ),
                aggregation_source=AggregationSource.EXTERNAL,
            )
            self.bar_types.append(bar_type)

            self.sma[instrument_id] = SimpleMovingAverage(20)
            self.register_indicator_for_bars(bar_type, self.sma[instrument_id])
            self.sma_values[instrument_id] = []

            self.subscribe_bars(bar_type)
            self.subscribe_trade_ticks(instrument_id)

            from_datetime = datetime(2021, 1, 4, 10, 30)
            until_datetime = datetime(2021, 1, 4, 19, 30)
            self.request_quote_ticks(instrument_id, from_datetime, until_datetime)

    def _calculate_buy_signal(self, instrument_id: InstrumentId, bar_type: BarType):
        last_close = self.cache.bar(bar_type, 0).close.as_double()
        prev_close = self.cache.bar(bar_type, 1).close.as_double()
        last_sma = self.sma_values[instrument_id][-1]
        prev_sma = self.sma_values[instrument_id][-2]

        if prev_close < prev_sma and last_close > last_sma:
            self.log.info(
                f'{instrument_id} - '
                f'buy signal: prev_close {prev_close} < prev_sma {prev_sma} '
                f'last_close {last_close} > last_sma {last_sma}')
            return True
        else:
            self.log.info(
                f'{instrument_id} - '
                f'closes = {last_close, prev_close}, sma = {last_sma, prev_sma}')
            return False

    def on_bar(self, bar: Bar):
        print(bar)
        instrument_id = bar.bar_type.instrument_id

        if self.sma[instrument_id].initialized:
            self.sma_values[instrument_id].append(self.sma[instrument_id].value)

        if self.cache.bar_count(bar.bar_type) > 1:
            self.buy(instrument_id)

        if len(self.sma_values[instrument_id]) < 2:
            self.log.info(
                f'{instrument_id} - Waiting for bars [{self.cache.bar_count(bar.bar_type)}]...',
                color=LogColor.BLUE,
            )
            return

        orders_count = self.cache.orders_open_count(instrument_id=instrument_id) + \
                       self.cache.orders_inflight_count(instrument_id=instrument_id)

        if self.portfolio.is_flat(instrument_id) and orders_count == 0:
            signal = self._calculate_buy_signal(instrument_id, bar.bar_type)
            if signal:
                self.buy(instrument_id)

    def on_event(self, event: Event):
        if isinstance(event, (PositionOpened, PositionChanged)):
            if event.instrument_id in self.instrument_ids:
                self.sell(event.instrument_id, event.position_id)

    def buy(self, instrument_id: InstrumentId):
        price: double = self.cache.trade_tick(instrument_id).price.as_double()
        quantity: int = int(self.budget / price)

        order: LimitOrder = self.order_factory.limit(
            instrument_id=instrument_id,
            order_side=OrderSide.BUY,
            quantity=self.cache.instrument(instrument_id).make_qty(quantity),
            price=Price(price, 2),
            time_in_force=TimeInForce.DAY,
        )

        self.submit_order(order)

    def sell(self, instrument_id: InstrumentId, position_id: PositionId):
        instrument: Instrument = self.cache.instrument(instrument_id)
        position: Position = self.cache.position(position_id)
        latest_price: double = self.cache.trade_tick(instrument_id).price.as_double()

        price = max(
            position.avg_px_open + 0.01,
            latest_price,
        )

        order: LimitOrder = self.order_factory.limit(
            instrument_id=instrument_id,
            order_side=OrderSide.SELL,
            quantity=instrument.make_qty(position.quantity),
            price=Price(price, 2),
            time_in_force=TimeInForce.DAY,
        )

        self.submit_order(order)

    def on_stop(self):
        """
        Actions to be performed when the strategy is stopped.
        """
        for instrument_id in self.instrument_ids:
            self.log.info(f'STOPPING... {str(instrument_id)}:')
            self.cancel_all_orders(instrument_id)
            self.close_all_positions(instrument_id)

        # Unsubscribe from data
        for bar_type in self.bar_types:
            self.unsubscribe_bars(bar_type)
