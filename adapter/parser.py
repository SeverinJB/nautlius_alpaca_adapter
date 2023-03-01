from decimal import Decimal
from typing import Any, Dict, Optional, List

import pandas as pd

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import TradeReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data.bar import Bar, BarType
from nautilus_trader.model.data.tick import TradeTick, QuoteTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

from nautilus_trader.model.enums import time_in_force_from_str

def parse_order_status_http(
        account_id: AccountId,
        instrument: Instrument,
        data: Dict[str, Any],
        ts_init: int,
        report_id: UUID4,
) -> OrderStatusReport:
    client_order_id = data.get('client_order_id')
    price = data.get('limit_price')
    avg_px = data['filled_avg_price']
    created_at = int(pd.to_datetime(data['created_at'], utc=True).to_datetime64())

    return OrderStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        client_order_id=ClientOrderId(client_order_id) if client_order_id is not None else None,
        venue_order_id=VenueOrderId(str(data['id'])),
        order_side=OrderSide.BUY if data['side'] == 'buy' else OrderSide.SELL,
        order_type=parse_order_type(data=data),
        time_in_force=time_in_force_from_str(data['time_in_force'].upper()),
        order_status=parse_order_status(data),
        price=instrument.make_price(price) if price is not None else None,
        quantity=instrument.make_qty(data['qty']),
        filled_qty=instrument.make_qty(data['filled_qty']),
        avg_px=Decimal(str(avg_px)) if avg_px is not None else None,
        report_id=report_id,
        ts_accepted=created_at,
        ts_last=created_at,
        ts_init=ts_init,
    )


def parse_bar_http(bar_type: BarType, data: dict, ts_init: int) -> Bar:
    created_at = int(pd.to_datetime(data['t'], utc=True).to_datetime64())

    return Bar(
        bar_type=bar_type,
        open=Price.from_int(data['o']),
        high=Price.from_int(data['h']),
        low=Price.from_int(data['l']),
        close=Price.from_int(data['c']),
        volume=Quantity.from_int(data['v']),
        ts_event=created_at,
        ts_init=ts_init,
    )


def parse_order_status(data: Dict[str, Any]) -> OrderStatus:
    status: Optional[str] = data.get('status')
    if status == 'new':
        if data['filled_qty'] == 0:
            return OrderStatus.ACCEPTED
        else:
            return OrderStatus.PARTIALLY_FILLED
    elif status == 'partially_filled':
        return OrderStatus.PARTIALLY_FILLED
    elif status == 'filled':
        return OrderStatus.FILLED
    elif status in ('canceled', 'replaced'):
        return OrderStatus.CANCELED # FIXME: Is this really the best for replaced?
    elif status == 'expired':
        return OrderStatus.EXPIRED
    elif status == 'pending_cancel':
        return OrderStatus.PENDING_CANCEL
    elif status == 'pending_update':
        return OrderStatus.PENDING_UPDATE
    elif status == 'rejected':
        return OrderStatus.REJECTED
    else:  # pragma: no cover (design-time error)
        raise RuntimeError(f"cannot parse order status, was {status}")


def parse_order_type(data: Dict[str, Any]) -> OrderType:
    order_type: str = data['type']
    if order_type == 'limit':
        return OrderType.LIMIT
    elif order_type == 'market':
        return OrderType.MARKET
    elif order_type in ('stop', 'trailing_stop', 'stop_limit'):
        if data.get('limit_price'):
            return OrderType.STOP_LIMIT
        else:
            return OrderType.STOP_MARKET
    else:  # pragma: no cover (design-time error)
        raise RuntimeError(f"cannot parse order type, was {order_type}")


def parse_position_report(
        account_id: AccountId,
        instrument: Instrument,
        data: Dict[str, Any],
        ts_init: int,
        report_id: UUID4,
) -> PositionStatusReport:
    return PositionStatusReport(
        account_id=account_id,
        instrument_id=instrument.id,
        position_side=PositionSide.LONG if data['side'] == 'long' else PositionSide.SHORT,
        quantity=instrument.make_qty(data['qty']),
        report_id=report_id,
        ts_last=ts_init,
        ts_init=ts_init,
    )


def parse_trade_report(
        account_id: AccountId,
        instrument: Instrument,
        data: Dict[str, Any],
        report_id: UUID4,
        ts_init: int,
) -> TradeReport:
    return TradeReport(
        account_id=account_id,
        instrument_id=instrument.id,
        venue_order_id=VenueOrderId(data['order_id']),
        trade_id=TradeId(data['id']),
        order_side=OrderSide.BUY if data['side'] == 'buy' else OrderSide.SELL,
        last_qty=instrument.make_qty(data['qty']),
        last_px=instrument.make_price(data['price']),
        liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
        report_id=report_id,
        ts_event=pd.to_datetime(data['transaction_time'], utc=True).value,
        ts_init=ts_init,
        commission=Money(0, USD),  # Bug #809
    )


def parse_trade_ticks(
    instrument: Instrument,
    data: List[Dict[str, Any]],
    ts_init: int,
) -> List[TradeTick]:
    trades = data['trades']
    ticks: List[TradeTick] = []
    for trade in trades:
        tick: TradeTick = TradeTick(
            instrument_id=instrument.id,
            price=instrument.make_price(trade['p']),
            size=instrument.make_qty(trade['s']),
            aggressor_side=AggressorSide.NO_AGGRESSOR,  # Looks like Side not in Alpaca data
            trade_id=TradeId(str(trade['i'])),
            ts_event=pd.to_datetime(trade['t'], utc=True).value,
            ts_init=ts_init,
        )
        ticks.append(tick)

    return ticks


def parse_quote_ticks(
    instrument: Instrument,
    data: List[Dict[str, Any]],
    ts_init: int,
) -> List[TradeTick]:
    ticks: List[QuoteTick] = []
    for quote in data:
        tick: QuoteTick = QuoteTick(
            instrument_id=instrument.id,
            ask=instrument.make_price(quote['ap']),
            bid=instrument.make_price(quote['bp']),
            ask_size=instrument.make_qty(quote['as']),
            bid_size=instrument.make_qty(quote['bs']),
            ts_event=pd.to_datetime(quote['t'], utc=True).value,
            ts_init=ts_init,
        )
        ticks.append(tick)

    return ticks