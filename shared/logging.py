import logging
import os
import sys


def configure_logging(service_name: str):
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s %(levelname)s [{service_name}] %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    for noisy in ("aio_pika", "aiormq", "httpx", "aiosmtplib", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    max_len = int(os.getenv("LOG_MAX_LEN", "2000"))
    factory = logging.getLogRecordFactory()

    def _factory(*args, **kwargs):
        record = factory(*args, **kwargs)
        try:
            msg = record.getMessage()
            if len(msg) > max_len:
                record.msg = msg[:max_len] + "...(truncated)"
                record.args = ()
        except Exception:
            pass
        return record

    logging.setLogRecordFactory(_factory)