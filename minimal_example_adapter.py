from adapter.config import AlpacaDataClientConfig, AlpacaExecClientConfig
from adapter.factories import AlpacaDataClientFactory, AlpacaExecClientFactory

from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode

from strategy import ScalpingStrategy
from strategy import ScalpingStrategyConfig

# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***

# *** THIS INTEGRATION IS STILL UNDER CONSTRUCTION. ***
# *** PLEASE CONSIDER IT TO BE IN AN UNSTABLE BETA PHASE AND EXERCISE CAUTION. ***

# Configure the trading node
config_node = TradingNodeConfig(
    trader_id='TESTER-001',
    log_level='DEBUG',
    data_clients={
        'ALPACA': AlpacaDataClientConfig(
            api_key='YOUR_KEY',
            api_secret='YOUR_SECRET',
            instrument_provider=InstrumentProviderConfig(
                load_ids=frozenset(['AAPL.ALPACA', 'IBM.ALPACA', 'TSLA.ALPACA'])
            )
        )
    },
    exec_clients={
        'ALPACA': AlpacaExecClientConfig(
            api_key='YOUR_KEY',
            api_secret='YOUR_SECRET',
            instrument_provider=InstrumentProviderConfig(
                load_ids=frozenset(['AAPL.ALPACA', 'IBM.ALPACA', 'TSLA.ALPACA'])
            )

        )
    },
    timeout_connection=90.0,
    timeout_reconciliation=5.0,
    timeout_portfolio=5.0,
    timeout_disconnection=5.0,
    timeout_post_stop=2.0,
)

# Instantiate the node with a configuration
node = TradingNode(config=config_node)

# Configure your strategy
strategy_config = ScalpingStrategyConfig(
    instrument_ids=['AAPL.ALPACA', 'IBM.ALPACA', 'TSLA.ALPACA'],
    budget=40000,
)
# Instantiate your strategy
strategy = ScalpingStrategy(config=strategy_config)

# Add your strategies_ideas and modules
node.trader.add_strategy(strategy)

# Register your client factories with the node (can take user defined factories)
node.add_data_client_factory("ALPACA", AlpacaDataClientFactory)
node.add_exec_client_factory("ALPACA", AlpacaExecClientFactory)
node.build()

# Stop and dispose of the node with SIGINT/CTRL+C
if __name__ == "__main__":
    try:
        node.start()
    finally:
        node.dispose()
