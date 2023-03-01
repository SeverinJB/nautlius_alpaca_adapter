from typing import Optional

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import InstrumentProviderConfig


class AlpacaDataClientConfig(LiveDataClientConfig):
    """
    Configuration for ''AlpacaDataClient'' instances.
    Parameters
    ----------
    api_key : str
        The Alpaca API public key.
    api_secret : str
        The Alpaca API public key.
    paper_trading : str, optional
        By default, paper_trading is True.
    subscription_plan: str, optional
        Subscription plan for market data. Supports 'iex' and 'sip'.
    instrument_provider: InstrumentProviderConfig
        Config for the instrument provider.
    """

    api_key: str = ''
    api_secret: str = ''
    paper_trading: Optional[bool] = True
    subscription_plan: Optional[str] = 'iex'
    instrument_provider: Optional[InstrumentProviderConfig] = None


class AlpacaExecClientConfig(LiveExecClientConfig):
    """
    Configuration for ''AlpacaExecutionClient'' instances.
    Parameters
    ----------
    api_key : str
        The Alpaca API public key.
    api_secret : str
        The Alpaca API public key.
    paper_trading : str, optional
        By default, paper_trading is True.
    instrument_provider: InstrumentProviderConfig
        Config for the instrument provider.
    """

    api_key: str = ''
    api_secret: str = ''
    paper_trading: Optional[bool] = True
    instrument_provider: Optional[InstrumentProviderConfig] = None
