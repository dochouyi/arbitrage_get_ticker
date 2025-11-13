import websocket
import json
import time
import argparse
import logging
from datetime import datetime
import redis
from utils.utils import *


previous_last_price=None


def save_ticker_to_redis(rds, symbol, last_price):
    """
    只推送ticker数据到 Redis Channel（不做缓存）
    """
    value = json.dumps({
        "last_price": last_price,
    }, ensure_ascii=False)
    rds.publish(f"bybit:channel:ticker:{symbol}", value)


def on_message(ws, message):
    global previous_last_price
    data = json.loads(message)
    topic = data.get("topic", "")
    if topic.startswith("tickers."):
        ticker = data.get("data", {})
        symbol = ticker.get("symbol") or ticker.get("s")
        last_price = ticker.get("lastPrice") or ticker.get("last_price") or ticker.get("lp")
        if last_price==None:
            last_price=previous_last_price
        else:
            previous_last_price=last_price
        if debug:
            logger.info(f"交易对: {symbol} 最新价: {last_price}")
        save_ticker_to_redis(rds, symbol, last_price)
    else:
        logger.warning(f"收到未知消息: {message}")

def on_error(ws, error):
    logger.error(f"Error: {error}")
    ws.close()

def on_close(ws, close_status_code, close_msg):
    logger.warning("### closed ###")

def on_open(ws):
    logger.info("WebSocket连接已打开。")
    args = [f"tickers.{sym.upper()}" for sym in symbols]
    sub_msg = {"op": "subscribe", "args": args}
    ws.send(json.dumps(sub_msg))
    logger.info(f"已订阅: {args}")

def run_ws(symbols_list, use_proxy=False, proxy_host=None, proxy_port=None, proxy_type=None):
    url = "wss://stream.bybit.com/v5/public/linear"
    logger.info(f"连接URL: {url}")
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    if use_proxy:
        ws.run_forever(
            http_proxy_host=proxy_host,
            http_proxy_port=proxy_port,
            proxy_type="socks5"
        )
    else:
        ws.run_forever()

if __name__ == "__main__":

    symbols = load_symbols_from_yaml("symbols_list.yml")
    config = read_config('config.yml')

    logger = setup_logger('bybit_ticker')

    rds = redis.Redis(host=config['redis_host'], port=config['redis_port'], db=config['redis_db'])
    debug = config['debug']
    symbols = symbols

    while True:
        try:
            run_ws(
                symbols_list=symbols,
                use_proxy=config['use_proxy'],
                proxy_host=config['proxy_host'],
                proxy_port=config['proxy_port'],
            )
        except Exception as e:
            logger.error(f"连接异常: {e}")
        logger.info("5秒后重试连接...")
        time.sleep(5)

# python ticker.py --use_proxy --proxy_host 127.0.0.1 --proxy_port 7891 --redis_host 127.0.0.1 --symbols btcusdt ethusdt --debug