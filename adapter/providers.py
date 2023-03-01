import time
from typing import Any, Dict, List, Optional

from adapter.http import AlpacaHttpClient
from adapter.core import ALPACA_VENUE

from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.currency import Currency
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.model.instruments.equity import Equity, Decimal
from nautilus_trader.model.objects import Quantity, Price
from nautilus_trader.common.logging import Logger
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.core.correctness import PyCondition


# The 'pragma: no cover' comment excludes a method from test coverage.
# https://coverage.readthedocs.io/en/coverage-4.3.3/excluding.html
# The reason for their use is to reduce redundant/needless tests which simply
# assert that a `NotImplementedError` is raised when calling abstract methods.
# These tests are expensive to maintain (as they must be kept in line with any
# refactorings), and offer little to no benefit in return. However, the intention
# is for all method implementations to be fully covered by tests.

# *** THESE PRAGMA: NO COVER COMMENTS MUST BE REMOVED IN ANY IMPLEMENTATION. ***


class AlpacaInstrumentProvider(InstrumentProvider):
    """
    An example of a ``LiveExecutionClient`` highlighting the method requirements.

    +----------------------------------+-------------+
    | Method                           | Requirement |
    +----------------------------------+-------------+
    | load_all_async                   | required - IN PROGRESS   |
    | load_ids_async                   | required - IN PROGRESS   |
    | load_async                       | required - IN PROGRESS   |
    +------------------------------------------------+

    Provides a means of loading 'Instrument' from the Alpaca API.

    Parameters
    ----------
    client : APIClient
        The client for the provider.
    logger : Logger
        The logger for the provider.
    config : InstrumentProviderConfig, optional
        The configuration for the provider.
    """

    def __init__(
            self,
            client: AlpacaHttpClient,
            logger: Logger,
            config: Optional[InstrumentProviderConfig] = None,
    ):
        super().__init__(
            venue=ALPACA_VENUE,
            logger=logger,
            config=config,
        )

        self._http_client = client

    async def load_all_async(self, filters: Optional[Dict] = None) -> None:
        filters_str = '...' if not filters else f' with filters {filters}...'
        self._log.info(f"Loading all instruments{filters_str}")

        account_info: Dict[str, Any] = await self._http_client.get_account_info()
        assets: List[Dict[str, Any]] = await self._http_client.list_markets()

        if filters is not None:
            raise NotImplementedError('Filter feature must be implemented.')

        for data in assets:
            if data['tradable']:
                self._parse_instrument(data, account_info['currency'], account_info['multiplier'])

    async def load_ids_async(
            self,
            instrument_ids: List[InstrumentId],
            filters: Optional[Dict] = None,
    ) -> None:
        if not instrument_ids:
            self._log.info("No instrument IDs given for loading.")
            return

        if filters is not None:
            raise NotImplementedError('Filter feature must be implemented.')

        filters_str = '...' if not filters else f' with filters {filters}...'
        self._log.info(f'Loading all instruments{filters_str}')

        account_info: Dict[str, Any] = await self._http_client.get_account_info()

        for instrument_id in instrument_ids:
            PyCondition.equal(instrument_id.venue, self.venue, 'instrument_id.venue', 'self.venue')

            data: Dict[str, Any] = await self._http_client.get_market(instrument_id.symbol.value)

            if data['tradable']:
                self._parse_instrument(data, account_info['currency'], account_info['multiplier'])
            else:
                self._log.warning(f'{instrument_id} not tradable on Alpaca.')

    async def load_async(self, instrument_id: InstrumentId, filters: Optional[Dict] = None):
        if filters is not None:
            raise NotImplementedError('Filter feature must be implemented.')

        PyCondition.not_none(instrument_id, 'instrument_id')
        PyCondition.equal(instrument_id.venue, self.venue, 'instrument_id.venue', 'self.venue')

        filters_str = '...' if not filters else f' with filters {filters}...'
        self._log.info(f'Loading all instrument {instrument_id}{filters_str}')

        account_info: Dict[str, Any] = await self._http_client.get_account_info()
        data: Dict[str, Any] = await self._http_client.get_market(instrument_id.symbol.value)

        if data['tradable']:
            self._parse_instrument(data, account_info['currency'], account_info['multiplier'])
        else:
            self._log.warning(f'{instrument_id} not tradable on Alpaca.')

    # -- PARSER -------------------------------------------------------------------------

    def _parse_instrument(
            self,
            data: Dict[str, Any],
            currency: str,
            multiplier: str,
    ) -> None:
        native_symbol = Symbol(data['symbol'])
        asset_type = data['class']

        instrument_id = InstrumentId(native_symbol, ALPACA_VENUE)
        timestamp = time.time_ns()

        if asset_type == 'us_equity':
            instrument: Instrument = Equity(
                instrument_id=instrument_id,
                native_symbol=native_symbol,
                currency=Currency.from_str(currency),
                price_precision=3,
                price_increment=Price(1, 3),
                multiplier=Quantity.from_str(multiplier),
                lot_size=Quantity.from_int(1),
                ts_event=timestamp,
                ts_init=timestamp,
                margin_maint=Decimal(data['maintenance_margin_requirement']),
            )
        else:
            self._log.info(f'Adapter does not support crypto, {instrument_id}')
            return

        self.add_currency(currency=instrument.quote_currency)
        self.add(instrument=instrument)
