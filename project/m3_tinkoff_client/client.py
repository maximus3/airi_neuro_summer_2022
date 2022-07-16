import logging
import time
from datetime import datetime
from typing import Optional

import requests
from tinkoff.invest import (
    CandleInterval,
    Client,
    HistoricCandle,
    Instrument,
    InstrumentIdType,
    RequestError,
)
from tinkoff.invest.exceptions import StatusCode

from .cache import CACHE, CANDLES_CACHE


class TinkoffClientByM3:
    CURRENCIES = {
        'USD': 'USD000UTSTOM',
        'EUR': 'EUR_RUB__TOM',
        'RUB': 'MYRUB_TICKER',
    }

    LINK_INSTRUMENT_BY_TICKER = (
        'https://api-invest.tinkoff.ru/trading/stocks/get?ticker={}'
    )

    STR_TO_CANDLE_INTERVAL = {
        '1m': CandleInterval.CANDLE_INTERVAL_1_MIN,
        '5m': CandleInterval.CANDLE_INTERVAL_5_MIN,
        '15m': CandleInterval.CANDLE_INTERVAL_15_MIN,
        '1h': CandleInterval.CANDLE_INTERVAL_HOUR,
        '1d': CandleInterval.CANDLE_INTERVAL_DAY,
    }

    def __init__(
        self,
        TOKEN: str,
        is_real: bool = False,
        debug: bool = False,
        diff: int = 1,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Parameters
        ----------
        TOKEN   : string
            TinkoffAPI token.
        is_real : bool
            Is it real token (True) or sandbox (False).
        debug   : bool
        diff    : int
            Diff in hours  #TODO: describe
        logger  :
            Logging object
        """
        self.isReal = is_real

        def _client_gen() -> Client:
            return Client(TOKEN)

        self._client_gen = _client_gen
        # if not is_real:
        #     self._client.sandbox.sandbox_remove_post()
        #     self._client.sandbox.sandbox_register_post()

        # # Operations
        # self.operations_data: dict[str, list] = {
        #     'BROKER': [],
        #     'IIS': []
        # }
        # self.lastOperationGet: dict[str, datetime] = {
        #     'BROKER': datetime.now() - timedelta(days=1),
        #     'IIS': datetime.now() - timedelta(days=1)
        # }
        # self.operation_diff = diff

        if logger is None:
            logger = logging.getLogger('TinkoffClientByM3')

        self.logger = logger

        if debug:
            self.run_tests()

    def run_tests(self) -> None:
        assert self.get_ticker_by_figi('BBG006L8G4H1') == 'YNDX'
        assert CACHE['BY_FIGI'].get('BBG006L8G4H1').ticker == 'YNDX'
        assert self.get_ticker_by_figi('BBG006L8G4H1') == 'YNDX'

        assert self.get_figi_by_ticker('YNDX') == 'BBG006L8G4H1'
        assert CACHE['BY_TICKER'].get('YNDX').figi == 'BBG006L8G4H1'
        assert self.get_figi_by_ticker('YNDX') == 'BBG006L8G4H1'

        assert self.get_name_by_ticker('YNDX') == 'Yandex'
        assert CACHE['BY_TICKER'].get('YNDX').name == 'Yandex'
        assert self.get_name_by_ticker('YNDX') == 'Yandex'

        self.logger.info('Tests OK')

    @staticmethod
    def _check_figi_or_ticker(
        figi: Optional[str], ticker: Optional[str]
    ) -> None:
        if (figi is None) and (ticker is None):
            raise RuntimeError('Give ticker or figi for operation')
        if (figi is not None) and (ticker is not None):
            raise RuntimeError('Give only one of ticker and figi')

    def _get_class_code(self, ticker: str) -> str:
        data = requests.get(
            self.LINK_INSTRUMENT_BY_TICKER.format(ticker)
        ).json()
        return data['payload']['symbol']['classCode']

    def _func_with_repeat(self, func, repeat, *args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except RequestError as exc:
                if exc.args[0] == StatusCode.RESOURCE_EXHAUSTED:
                    if repeat:
                        time_to_sleep = exc.args[2].ratelimit_reset + 1
                        self.logger.info(
                            f'{exc.args[0]}, sleeping for {time_to_sleep} seconds'
                        )
                        time.sleep(time_to_sleep)
                        continue
                    raise
                else:
                    raise

    def _get_instrument_by(
        self, id_type: InstrumentIdType, instr_id: str, repeat: bool = False
    ) -> Instrument:
        class_code = ''
        if id_type == InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER:
            class_code = self._get_class_code(instr_id)

        def tmp_func(*args, **kwargs):
            with self._client_gen() as client:
                instrument = client.instruments.get_instrument_by(
                    *args, **kwargs
                ).instrument
                CACHE['BY_FIGI'].put(instrument.figi, instrument)
                CACHE['BY_TICKER'].put(instrument.ticker, instrument)
                return instrument

        return self._func_with_repeat(
            tmp_func,
            repeat,
            id_type=id_type,
            class_code=class_code,
            id=instr_id,
        )

    def get_ticker_by_figi(self, figi: str, repeat: bool = False) -> str:
        result_in_cache = CACHE['BY_FIGI'].get(figi)
        if result_in_cache is not None:
            return result_in_cache.ticker
        instrument = self._get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
            instr_id=figi,
            repeat=repeat,
        )
        return instrument.ticker

    def get_figi_by_ticker(self, ticker: str, repeat: bool = False) -> str:
        if ticker in self.CURRENCIES:
            ticker = self.CURRENCIES[ticker]
        result_in_cache = CACHE['BY_TICKER'].get(ticker)
        if result_in_cache:
            return result_in_cache.figi
        instrument = self._get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
            instr_id=ticker,
            repeat=repeat,
        )
        return instrument.figi

    def get_name_by_ticker(self, ticker: str, repeat: bool = False) -> str:
        result_in_cache = CACHE['BY_TICKER'].get(ticker)
        if result_in_cache:
            return result_in_cache.name
        instrument = self._get_instrument_by(
            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
            instr_id=ticker,
            repeat=repeat,
        )
        return instrument.name

    def _str_to_candle_interval(self, interval: str) -> CandleInterval:
        candle_interval = self.STR_TO_CANDLE_INTERVAL.get(interval)
        if candle_interval is None:
            self.logger.error('No such interval %s', interval)
            raise ValueError(
                f'No such interval {interval}',
                *list(self.STR_TO_CANDLE_INTERVAL.keys()),
            )
        return candle_interval

    def get_candles(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
        figi: Optional[str] = None,
        ticker: Optional[str] = None,
        interval: str = 'day',
        repeat: bool = False,
    ) -> list[HistoricCandle]:
        self._check_figi_or_ticker(figi, ticker)
        candle_interval = self._str_to_candle_interval(interval)
        if ticker:
            figi = self.get_figi_by_ticker(ticker, repeat=repeat)
        else:
            ticker = self.get_ticker_by_figi(figi, repeat=repeat)
        cached = CANDLES_CACHE.get(
            ticker=ticker, interval=interval, start=from_date, end=to_date
        )
        if cached is not None:
            return cached

        def tmp_func(*args, **kwargs):
            with self._client_gen() as client:
                return client.market_data.get_candles(*args, **kwargs).candles

        data = self._func_with_repeat(
            tmp_func,
            repeat,
            figi=figi,
            from_=from_date,
            to=to_date,
            interval=candle_interval,
        )
        CANDLES_CACHE.push(
            data,
            ticker=ticker,
            interval=interval,
            start=from_date,
            end=to_date,
        )
        return data


#     def get_price_by_ticker(self, ticker, day=None, repeat=False):
#         if ticker == 'RUB':
#             return 1
#         figi = self.get_figi_by_ticker(ticker)
#         if day:
#             day_st = day
#             if ticker in CACHE['PRICE']:
#                 if day in CACHE['PRICE'][ticker]:
#                     return CACHE['PRICE'][ticker][day]
#             else:
#                 CACHE['PRICE'][ticker] = {}
#             data = []
#             while len(data) == 0:
#                 day_from = day - timedelta(days=1)
#                 data = None
#                 while data is None:
#                     try:
#                         data = self._client.market.market_candles_get(figi,
#                                                                       _from=day_from.isoformat(),
#                                                                       to=day.isoformat(),
#                                                                       interval='day').payload.candles
#                     except ApiException as exc:
#                         if exc.status == 429:
#                             if repeat:
#                                 self.logger.info(f'{exc.reason}, sleeping for {self.TINKOFF_API_SLEEP_TIME} seconds')
#                                 time.sleep(self.TINKOFF_API_SLEEP_TIME)
#                                 continue
#                             raise
#                         else:
#                             raise
#
#                 day -= timedelta(days=1)
#             CACHE['PRICE'][ticker][day_st] = data[0].c
#             return data[0].c
#         while True:
#             try:
#                 return self._client.market.market_orderbook_get(figi, 0).payload.last_price
#             except ApiException as exc:
#                 if exc.status == 429:
#                     if repeat:
#                         self.logger.info(f'{exc.reason}, sleeping for {self.TINKOFF_API_SLEEP_TIME} seconds')
#                         time.sleep(self.TINKOFF_API_SLEEP_TIME)
#                         continue
#                     raise
#                 else:
#                     raise
#
#     def get_broker_account_id(self, broker_type, repeat=False):
#         accounts = None
#         while accounts is None:
#             try:
#                 accounts = self._client.user.user_accounts_get().payload.accounts
#             except ApiException as exc:
#                 if exc.status == 429:
#                     if repeat:
#                         self.logger.info(f'{exc.reason}, sleeping for {self.TINKOFF_API_SLEEP_TIME} seconds')
#                         time.sleep(self.TINKOFF_API_SLEEP_TIME)
#                         continue
#                     raise
#                 else:
#                     raise
#         brokerAccountId = None
#         for acc in accounts:
#             if acc.broker_account_type == broker_type:
#                 brokerAccountId = acc.broker_account_id
#                 break
#         return brokerAccountId
#
#     def get_all_operations(self, from_date, to_date, broker_type='Tinkoff', with_cache=False, repeat=False):
#         broker = 'BROKER' if (broker_type is None or broker_type == 'Tinkoff') else 'IIS'
#         if with_cache and datetime.now() - timedelta(hours=self.operation_diff) < self.lastOperationGet[broker]:
#             return self.operations_data[broker]
#
#         if broker_type:
#             brokerAccountId = self.get_broker_account_id(broker_type)
#             if brokerAccountId is None:
#                 return []
#             data = None
#             while data is None:
#                 try:
#                     data = self._client.operations.operations_get(_from=from_date, to=to_date,
#                                                                   broker_account_id=brokerAccountId)
#                 except ApiException as exc:
#                     if exc.status == 429:
#                         if repeat:
#                             self.logger.info(f'{exc.reason}, sleeping for {self.TINKOFF_API_SLEEP_TIME} seconds')
#                             time.sleep(self.TINKOFF_API_SLEEP_TIME)
#                             continue
#                         raise
#                     else:
#                         raise
#         else:
#             data = None
#             while data is None:
#                 try:
#                     data = data = self._client.operations.operations_get(_from=from_date, to=to_date)
#                 except ApiException as exc:
#                     if exc.status == 429:
#                         if repeat:
#                             self.logger.info(f'{exc.reason}, sleeping for {self.TINKOFF_API_SLEEP_TIME} seconds')
#                             time.sleep(self.TINKOFF_API_SLEEP_TIME)
#                             continue
#                         raise
#                     else:
#                         raise
#
#         if with_cache:
#             self.operations_data[broker] = data.payload.operations
#             self.lastOperationGet[broker] = datetime.now()
#         return data.payload.operations
#
#     def get_orderbook(self, figi=None, ticker=None, depth=20):
#         self._check_figi_or_ticker(figi, ticker)
#         if ticker:
#             figi = self.get_figi_by_ticker(ticker)
#         return self._client.market.market_orderbook_get(figi, depth).payload
#
#
#     def sandbox_set_currency(self, currency, balance, remove=True):
#         if self.isReal == True:
#             raise RuntimeError("It is not sandbox token")
#         if remove:
#             self._client.sandbox.sandbox_remove_post()
#             self._client.sandbox.sandbox_register_post()
#         return self._client.sandbox.sandbox_currencies_balance_post({
#             "currency": currency,
#             "balance": balance
#         })
#
#     def get_balance(self, ticker=None, currency=None):
#         if ticker is None and currency is None:
#             raise RuntimeError('All args is None')
#         if ticker is not None and currency is not None:
#             raise RuntimeError('Only one arg should be not None')
#         if currency:
#             for cur in self._client.portfolio.portfolio_currencies_get().payload.currencies:
#                 if cur.currency == currency:
#                     return cur.balance
#         if ticker:
#             for pos in self._client.portfolio.portfolio_get().payload.positions:
#                 if pos.ticker == ticker:
#                     return pos.balance
#         return 0
#
#     @staticmethod
#     def get_orderbook_stat(orderbook):
#         quantity = 0
#         min_val = orderbook[0].price
#         max_val = orderbook[0].price
#         for elem in orderbook:
#             quantity += elem.quantity
#             min_val = min(elem.price, min_val)
#             max_val = max(elem.price, max_val)
#         return quantity, min_val, max_val
#
#     def get_prices(self, ticker):
#         payload = self.get_orderbook(ticker=ticker, depth=1)
#         buy_orderbook = payload.bids
#         sell_orderbook = payload.asks
#
#         buy_quantity, _, buy_price = self.get_orderbook_stat(buy_orderbook)
#         sell_quantity, sell_price, _ = self.get_orderbook_stat(sell_orderbook)
#         return buy_price, sell_price
#
#
# class TinkoffClientByM3WithOrders(TinkoffClientByM3):
#     def make_order(self, operation, figi=None, ticker=None, lots=0, is_market_order=False, price=None):
#         if (figi is None) and (ticker is None):
#             raise RuntimeError("Give ticker or figi for operation")
#         if (figi is not None) and (ticker is not None):
#             raise RuntimeError("Give only one of ticker and figi")
#         if ticker:
#             figi = self.get_figi_by_ticker(ticker)
#         if not is_market_order and price is None:
#             raise RuntimeError("Limit order need price argument")
#         if is_market_order:
#             result = self._client.orders.orders_market_order_post(figi, {
#                 "lots": lots,
#                 "operation": operation
#             })
#         else:
#             result = self._client.orders.orders_limit_order_post(figi, {
#                 "lots": lots,
#                 "operation": operation,
#                 "price": price
#             })
#         if result.status == "Ok":
#             return True, result.payload
#         else:
#             return False, result.payload
#
#     def buy(self, *args, **kwargs):
#         return self.make_order("Buy", *args, **kwargs)
#
#     def sell(self, *args, **kwargs):
#         return self.make_order("Sell", *args, **kwargs)
