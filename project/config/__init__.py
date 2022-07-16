import datetime as dt
import logging
from pathlib import Path

from pydantic import BaseSettings, Field
from pytz import timezone

BASE_DIR = Path(__file__).resolve().parent.parent


class ConfigData(BaseSettings):
    TOKEN_RO_ROBOOTEST: str = Field(None, env='TINKOFF_RO_TOKEN')
    ROBOTEST_ACC_ID: str = Field(None, env='TINKOFF_RO_ACC_ID')
    tzinfo = dt.datetime.now(tz=timezone('Europe/Moscow')).tzinfo
    logs_dir: Path = Field(BASE_DIR / 'logs', env='logs_dir')
    cache_dir: Path = Field(BASE_DIR / 'cache', env='cache_dir')

    class Config:
        env_file: Path = BASE_DIR / '.env'


cfg = ConfigData()
cfg.logs_dir.mkdir(exist_ok=True)
cfg.cache_dir.mkdir(exist_ok=True)


logging_format = '%(filename)s %(funcName)s [LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s] %(name)s: %(message)s'

logging.basicConfig(
    format=logging_format,
    level=logging.INFO,
    filename='logs.log',
)
