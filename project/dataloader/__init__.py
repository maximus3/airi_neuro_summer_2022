import datetime as dt
import logging
from itertools import count
from typing import Any, Optional

import pandas as pd
import yfinance
from tinkoff.invest import HistoricCandle, Quotation
# from tqdm.autonotebook import tqdm
from m3tqdm import tqdm

from config import cfg
from m3_tinkoff_client.client import TinkoffClientByM3

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class DataLoader:
    def __init__(
        self,
        datareader: str = 'tinkoff',
        client: Optional[TinkoffClientByM3] = None,
    ) -> None:
        self._all_datareaders = {
            'yahoo': self._get_data_yahoo,
            'yfinance': self._get_data_yfinance,
            'tinkoff': self._get_data_tinkoff,
        }
        self._all_intervals = {
            'yahoo': ['1d', '1wk', '1mo'],
            'yfinance': [
                '1m',
                '2m',
                '5m',
                '15m',
                '30m',
                '60m',
                '90m',
                '1h',
                '1d',
                '5d',
                '1wk',
                '1mo',
                '3mo',
            ],
            'tinkoff': ['1m', '5m', '15m', '1h', '1d'],
        }

        self.datareader = datareader
        if datareader == 'tinkoff' and client is None:
            if client is None:
                client = TinkoffClientByM3(
                    cfg.TOKEN_RO_ROBOOTEST, is_real=False
                )

        self.client = client

        if self._all_datareaders.get(self.datareader) is None:
            logger.error('No such datareader %s', self.datareader)
            raise ValueError(
                f'No such datareader {self.datareader}',
                *list(self._all_datareaders.keys()),
            )

        self._intervals = self._all_intervals[self.datareader]

        logger.info('DataLoader %s created', self.datareader)

    @staticmethod
    def _quotation_to_float(quo: Quotation) -> float:
        return float(f'{quo.units}.{quo.nano}')

    def _candle_to_dict(self, elem: HistoricCandle) -> dict[str, Any]:
        return {
            'time': elem.time,
            'open': self._quotation_to_float(elem.open),
            'high': self._quotation_to_float(elem.high),
            'low': self._quotation_to_float(elem.low),
            'close': self._quotation_to_float(elem.close),
            'volume': elem.volume,
            'is_complete': elem.is_complete,
        }

    def _get_data_tinkoff(
        self, ticker: str, start: dt.datetime, end: dt.datetime, interval: str
    ) -> Optional[pd.DataFrame]:
        if ticker.endswith('-USD'):  # CRYPTO
            logger.info('Ticker %s is crypto, use yfinance', ticker)
            return self._get_data_yfinance(ticker, start, end, interval)
        if self.client is None:
            return None
        data = self.client.get_candles(
            ticker=ticker,
            from_date=start,
            to_date=end,
            interval=interval,
            repeat=True,
        )
        data_list = list(map(self._candle_to_dict, data))
        data_df = pd.DataFrame(data_list)
        if 'time' in data_df.columns:
            data_df = data_df.set_index('time')
        return data_df

    def _get_data_yahoo(
        self, ticker: str, start: dt.datetime, end: dt.datetime, interval: str
    ) -> Optional[pd.DataFrame]:
        if ticker.endswith('-USD'):  # CRYPTO
            logger.info('Ticker %s is crypto, use yfinance', ticker)
            return self._get_data_yfinance(ticker, start, end, interval)
        url = (
            'https://query1.finance.yahoo.com/v7/finance/download/'
            f'{ticker}?period1={int(start.timestamp())}&period2={int(end.timestamp())}'
            '&interval=1d&events=history&includeAdjustedClose=true'
        )
        return pd.read_csv(url, index_col='Date')

    def _get_data_yfinance(
        self, ticker: str, start: dt.datetime, end: dt.datetime, interval: str
    ) -> Optional[pd.DataFrame]:
        if interval.endswith('min'):
            interval = interval.replace('min', 'm')
        return yfinance.download(
            ticker, start=start, end=end, interval=interval, progress=False
        )

    def get_data(
        self, ticker: str, start: dt.datetime, end: dt.datetime, interval: str
    ) -> Optional[pd.DataFrame]:
        if interval not in self._intervals:
            logger.error('No such interval %s', interval)
            raise ValueError(
                f'No such interval {interval}',
                *self._intervals,
            )
        return self._all_datareaders[self.datareader](
            ticker, start, end, interval
        )

    @staticmethod
    def _add_datetime(date, days=None, years=None) -> dt.datetime:
        if days:
            return date + dt.timedelta(days=1)
        if years:
            return dt.datetime(date.year + 1, date.month, date.day)
        raise RuntimeError('days and years is None')

    def get_data_less_day(
        self,
        ticker: str,
        start: dt.datetime,
        end: dt.datetime,
        interval: str = '1m',
    ) -> Optional[pd.DataFrame]:
        cur_start = start
        add_kwargs = {'days': 1}
        if interval == '1d':
            add_kwargs = {'years': 1}
        logger.info(
            'Getting %s for 1 %s (%s - %s)',
            ticker,
            list(add_kwargs.keys())[0],
            start,
            end,
        )
        cur_end = self._add_datetime(cur_start, **add_kwargs)
        if cur_end > end:
            cur_end = end
        data = self.get_data(ticker, cur_start, cur_end, interval)
        cur_start = cur_end
        for _ in tqdm(count(), total=(end - start).days):
            if cur_start >= end:
                break
            cur_end = self._add_datetime(cur_start, **add_kwargs)
            new_data = self.get_data(
                ticker,
                cur_start,
                cur_end,
                interval,
            )
            data = pd.concat([data, new_data]).sort_index()
            cur_start = cur_end
        return data
