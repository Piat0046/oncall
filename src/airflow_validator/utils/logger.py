"""로깅 설정"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(verbose: bool = False, log_dir: str = "./logs") -> logging.Logger:
    """애플리케이션 로거 설정"""
    logger = logging.getLogger("airflow_validator")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # 이미 핸들러가 설정되어 있으면 반환
    if logger.handlers:
        return logger

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # 파일 핸들러
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(
        log_path / f"validator_{datetime.now().strftime('%Y%m%d')}.log",
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(console_format)
    logger.addHandler(file_handler)

    return logger
