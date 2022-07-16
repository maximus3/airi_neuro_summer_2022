import logging

from config import cfg

logging.basicConfig(
    format='%(filename)s %(funcName)s [LINE:%(lineno)d]# %(levelname)-8s '
    '[%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO,
    filename=cfg.logs_dir / 'm3_tinkoff_client.log',
)
