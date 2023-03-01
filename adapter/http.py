import json
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import msgspec
from aiohttp import ClientResponse
from aiohttp import ClientResponseError

from nautilus_trader.network.http import HttpClient


class AlpacaHttpClient(HttpClient):
    """
    Provides an 'Alpaca' asynchronous HTTP client.
    """

    def __init__(
            self,
            loop,
            clock,
            logger,
            key: Optional[str] = None,
            secret: Optional[str] = None,
            base_url: Optional[str] = None,
    ):
        super().__init__(
            loop=loop,
            logger=logger,
        )
        self._clock = clock
        self._key = key
        self._secret = secret
        self._base_url = base_url

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def api_key(self) -> str:
        return self._key

    @property
    def api_secret(self) -> str:
        return self._secret

    @staticmethod
    def _prepare_payload(payload: Dict[str, str]) -> Optional[str]:
        return json.dumps(payload, separators=(",", ":")) if payload else None

    @staticmethod
    def _url_encode(params: Dict[str, str]) -> str:
        return "?" + urllib.parse.urlencode(params) if params else ""

    async def _send_request(
            self,
            http_method: str,
            url_path: str,
            data_request: bool = False,
            payload: Dict[str, str] = None,
            params: Dict[str, Any] = None,
    ) -> Any:
        headers = {}
        headers['APCA-API-KEY-ID'] = self._key
        headers['APCA-API-SECRET-KEY'] = self._secret

        if payload is None:
            payload = {}

        if data_request:
            base_url = 'https://data.alpaca.markets/v2'
        else:
            base_url = self._base_url

        query = self._url_encode(params)
        try:
            resp: ClientResponse = await self.request(
                method=http_method,
                url=base_url + url_path + query,
                headers=headers,
                data=self._prepare_payload(payload),
            )
        except ClientResponseError as e:
            print(e)
            return

        try:
            data = msgspec.json.decode(resp.data)
            return data
        except msgspec.MsgspecError:
            self._log.error(f"Could not decode data to JSON: {resp.data}.")

    async def get_trades(self, market: str) -> List[Dict[str, Any]]:
        return await self._send_request('GET', f'/stocks/{market}/trades', data_request=True)

    async def get_account_info(self) -> Dict[str, Any]:
        return await self._send_request("GET", "/v2/account")

    async def get_market(self, market: str) -> Dict[str, Any]:
        return await self._send_request("GET", f"/v2/assets/{market}")

    async def list_markets(self) -> List[Dict[str, Any]]:
        return await self._send_request("GET", "/v2/assets")

    async def get_open_orders(
            self,
            market: str = None,
            after: str = None,
            until: str = None,
            params: dict = None,
    ) -> List[Dict[str, Any]]:
        if params is None:
            params = dict()
        if after is not None:
            params['after'] = str(after)
        if until is not None:
            params['until'] = str(until)
        if market is not None:
            params['symbols'] = market
        return await self._send_request('GET', '/v2/orders', params=params)

    async def get_order_history(
            self,
            market: str = None,
            after: str = None,
            until: str = None,
            params: dict = None,
    ) -> List[Dict[str, Any]]:
        if params is None:
            params = dict()
        if after is not None:
            params['after'] = str(after)
        if until is not None:
            params['until'] = str(until)
        if market is not None:
            params['symbols'] = market
        params['status'] = 'all'
        return await self._send_request('GET', '/v2/orders', params=params)

    async def get_order_status(self, order_id: str) -> Dict[str, Any]:
        return await self._send_request('GET', f'/v2/orders/{order_id}')

    async def get_order_status_by_client_id(self, client_order_id: str) -> Dict[str, Any]:
        params = {'client_order_id': client_order_id}
        return await self._send_request('GET', f'/v2/orders:by_client_order_id', params=params)

    async def modify_order(
            self,
            client_order_id: str,
            price: Optional[str] = None,
            size: Optional[str] = None,
    ) -> dict:
        raise NotImplementedError

    async def place_order(self, data: Dict) -> Dict[str, Any]:
        return await self._send_request('POST', '/v2/orders', payload=data)

    async def place_trigger_order(
            self,
            market: str,
            side: str,
            size: str,
            order_type: str,
            client_id: str = None,
            price: Optional[str] = None,
            trigger_price: Optional[str] = None,
            trail_value: Optional[str] = None,
            reduce_only: bool = False,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    async def cancel_order(self, order_id: str) -> None:  # No response from Alpaca
        await self._send_request('DELETE', f'/v2/orders/{order_id}')
        return None

    async def cancel_order_by_client_id(self, client_order_id: str) -> None:
        order = await self.get_order_status_by_client_id(client_order_id)
        return await self.cancel_order(order["id"])

    async def cancel_all_orders(self, market: str) -> None:
        open_orders = await self.get_open_orders(market=market)
        order_ids = [order['id'] for order in open_orders]

        for order_id in order_ids:
            return await self.cancel_order(order_id)

    async def get_fills(
            self,
            after: Optional[str] = None,
            until: Optional[str] = None,
            params: dict = None,
    ) -> List[dict]:
        if params is None:
            params = dict()
        if after is not None:
            params['after'] = str(after)
        if until is not None:
            params['until'] = str(until)
        return await self._send_request('GET', '/v2/account/activities/FILL', params=params)

    async def get_positions(self) -> List[dict]:
        return await self._send_request(http_method='GET', url_path='/v2/positions')

    async def get_position(self, symbol: str) -> dict:
        return await self._send_request('GET', f'/v2/positions/{symbol}')

    async def get_all_trades(
            self, market: str, start_time: float = None, end_time: float = None
    ) -> List:
        raise NotImplementedError

    async def get_bars(
            self,
            market: str,
            timeframe: str,
            start: Optional[datetime] = None,
            end: Optional[datetime] = None,
            limit: Optional[int] = None,
            params: Dict = None,
    ) -> List[Dict[str, Any]]:
        if params is None:
            params = dict()
        if timeframe is not None:
            params['timeframe'] = str(timeframe)
        if start is not None:
            params['start'] = start.isoformat() + "Z"
        if end is not None:
            params['end'] = end.isoformat() + "Z"
        if limit is not None:
            params['limit'] = limit

        response = await self._send_request('GET', f'/stocks/{market}/bars',
                                            params=params, data_request=True)
        return response['bars']

    async def get_quote_ticks(
            self,
            market: str,
            start: Optional[datetime] = None,
            end: Optional[datetime] = None,
            limit: Optional[int] = None,
            params: Dict = None,
    ) -> List[Dict[str, Any]]:
        if params is None:
            params = dict()
        if start is not None:
            params['start'] = start.isoformat() + "Z"
        if end is not None:
            params['end'] = end.isoformat() + "Z"
        if limit is not None:
            params['limit'] = limit

        response = await self._send_request('GET', f'/stocks/{market}/quotes',
                                            params=params, data_request=True)
        return response['quotes']
