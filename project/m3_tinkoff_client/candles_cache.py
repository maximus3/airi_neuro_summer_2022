import datetime as dt
import logging
import pickle
from pathlib import Path

logger = logging.getLogger(__name__)


class CandlesCache:
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir
        self.cache_data = set()
        self.update_cache()

    def update_cache(self):
        self.cache_data = set()
        for filename in self.cache_dir.glob('*.pkl'):
            self.cache_data.add(filename)

    def _params_to_name(
        self, ticker: str, start: dt.datetime, end: dt.datetime, interval: str
    ) -> Path:
        return (
            self.cache_dir
            / f'{ticker}_{interval}_{start.isoformat().replace(":", "")}_{end.isoformat().replace(":", "")}.pkl'
        )

    def push(self, data, **kwargs):
        filename = self._params_to_name(**kwargs)
        with open(filename, 'wb') as f:
            pickle.dump(data, f)
        self.cache_data.add(filename)

    def get(self, **kwargs):
        filename = self._params_to_name(**kwargs)
        if filename not in self.cache_data or not filename.exists():
            return None
        try:
            with open(filename, 'rb') as f:
                data = pickle.load(f)
        except Exception:
            logger.exception('Error in pickle.load')
            self.cache_data.remove(filename)
            return None
        logger.info('Loaded from cache')
        return data
