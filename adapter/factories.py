import asyncio
from functools import lru_cache
from typing import Dict, Optional

from adapter.config import AlpacaDataClientConfig
from adapter.config import AlpacaExecClientConfig
from adapter.data import AlpacaDataClient
from adapter.execution import AlpacaExecutionClient
from adapter.http import AlpacaHttpClient
from adapter.providers import AlpacaInstrumentProvider
from adapter.core import ALPACA_VENUE

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.clock import LiveClock
from nautilus_trader.common.logging import LiveLogger
from nautilus_trader.common.logging import Logger
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory
from nautilus_trader.msgbus.bus import MessageBus
from nautilus_trader.model.identifiers import AccountId


HTTP_CLIENTS: Dict[str, AlpacaHttpClient] = {}

def get_cached_alpaca_http_client(
    key: str,
    secret: str,
    loop: asyncio.AbstractEventLoop,
    clock: LiveClock,
    logger: Logger,
    paper_trading: Optional[bool] = True,
) -> AlpacaHttpClient:
    """
    Cache and return an Alpaca HTTP client with the given key or secret.
    If a cached client with matching key and secret already exists, then that
    cached client will be returned.
    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    clock : LiveClock
        The clock for the client.
    logger : Logger
        The logger for the client.
    key : str
        The API key for the client.
    secret : str
        The API secret for the client.
    paper_trading : bool, optional
        The info whether paper or live trading.
    Returns
    -------
    AlpacaHttpClient
    """
    global HTTP_CLIENTS

    key = key
    secret = secret

    if paper_trading:
        base_url = 'https://paper-api.alpaca.markets'
    else:
        base_url = 'https://api.alpaca.markets'

    client_key: str = "|".join((key, secret))
    if client_key not in HTTP_CLIENTS:
        client = AlpacaHttpClient(
            key=key,
            secret=secret,
            loop=loop,
            clock=clock,
            logger=logger,
            base_url=base_url
        )
        HTTP_CLIENTS[client_key] = client
    return HTTP_CLIENTS[client_key]


@lru_cache(1)
def get_cached_alpaca_instrument_provider(
    client: AlpacaHttpClient,
    logger: Logger,
    config: InstrumentProviderConfig,
) -> AlpacaInstrumentProvider:
    """
    Cache and return an AlpacaInstrumentProvider.
    If a cached provider already exists, then that provider will be returned.
    Parameters
    ----------
    client : AlpacaHttpClient
        The client for the instrument provider.
    logger : Logger
        The logger for the instrument provider.
    config : InstrumentProviderConfig
        The configuration for the instrument provider.
    Returns
    -------
    AlpacaInstrumentProvider
    """
    return AlpacaInstrumentProvider(
        client=client,
        logger=logger,
        config=config,
    )


class AlpacaDataClientFactory(LiveDataClientFactory):
    """
    Provides an 'Alpaca' live data client factory.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: LiveLogger,
    ) -> AlpacaDataClient:
        """
        Create a new Alpaca data client.
        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : FTXDataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.
        logger : LiveLogger
            The logger for the client.
        Returns
        -------
        AlpacaDataClient
        """
        client = get_cached_alpaca_http_client(
            key=config.api_key,
            secret=config.api_secret,
            loop=loop,
            clock=clock,
            logger=logger,
            paper_trading=config.paper_trading,
        )

        # Get instrument provider singleton
        provider = get_cached_alpaca_instrument_provider(
            client=client,
            logger=logger,
            config=config.instrument_provider,
        )

        # Create client
        data_client = AlpacaDataClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            subscription_plan=config.subscription_plan,
            instrument_provider=provider,
        )
        return data_client


class AlpacaExecClientFactory(LiveExecClientFactory):
    """
    Provides an 'Alpaca' live execution client factory.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: AlpacaExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: LiveLogger,
    ) -> AlpacaExecutionClient:
        """
        Create a new Alpaca execution client.
        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : FTXExecClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.
        logger : LiveLogger
            The logger for the client.
        Returns
        -------
        AlpacaExecutionClient
        """
        client = get_cached_alpaca_http_client(
            key=config.api_key,
            secret=config.api_secret,
            loop=loop,
            clock=clock,
            logger=logger,
            paper_trading=config.paper_trading,
        )

        # Get instrument provider singleton
        provider = get_cached_alpaca_instrument_provider(
            client=client,
            logger=logger,
            config=config.instrument_provider,
        )

        # Create client
        exec_client = AlpacaExecutionClient(
            loop=loop,
            client=client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            paper_trading=config.paper_trading,
            instrument_provider=provider,
            account_id=AccountId(f'{ALPACA_VENUE.value}-001'),
        )
        return exec_client
