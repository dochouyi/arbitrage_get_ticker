import logging
import os
import yaml
from typing import Dict, Optional, List


def setup_logger(log_file='binance_aggtrade.log'):
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger



def load_symbols_from_yaml(path: str) -> list:
    if not os.path.exists(path):
        raise FileNotFoundError(f"YAML config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    symbols = data.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("YAML must contain a non-empty list under key 'symbols'")
    symbols = [str(s).strip().lower() for s in symbols if str(s).strip()]
    if not symbols:
        raise ValueError("No valid symbols found in YAML")
    return symbols


def build_proxies(use_proxy: bool, proxy_host: Optional[str], proxy_port: Optional[int]) -> Optional[Dict[str, str]]:
    if use_proxy and proxy_host and proxy_port:
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        return {"http": proxy_url, "https": proxy_url}
    return None


def read_config(config_path):
    """
    读取 YAML 格式的配置文件并返回为字典
    :param config_path: 配置文件路径
    :return: 配置字典
    """
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config


def convert_symbol(symbol):
    """
    将币对名称从 binance 格式转为 okx 格式
    例如 btcusdt -> BTC-USDT-SWAP
    """
    base = symbol[:-4].upper()
    quote = symbol[-4:].upper()
    return f"{base}-{quote}-SWAP"