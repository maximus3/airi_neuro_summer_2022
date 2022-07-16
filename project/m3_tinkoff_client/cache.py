import logging
from typing import Any

from config import cfg

from .candles_cache import CandlesCache
from .data_cache import DataCache

logger = logging.getLogger(__name__)

CACHE: dict[str, Any] = {
    'BY_TICKER': DataCache(),  # TICKER - INSTRUMENT
    'BY_FIGI': DataCache(),  # FIGI - INSTRUMENT
    'PRICE': {},
}
CANDLES_CACHE = CandlesCache(cfg.cache_dir)
logger.info('CACHE loaded')
