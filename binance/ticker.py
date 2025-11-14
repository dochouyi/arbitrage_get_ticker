import websocket
import json
import time
import argparse
from datetime import datetime
import redis
from utils.utils import *



def save_ticker_to_redis(rds, symbol, last_price):
    """
    只推送ticker数据到 Redis Channel（不做缓存）
    """
    value = json.dumps({
        "last_price": last_price,
    }, ensure_ascii=False)
    rds.publish(f"binance:channel:ticker:{symbol}", value)

def on_message(ws, message):
    data = json.loads(message)

    if 'data' in data and 'stream' in data:
        ticker = data['data']
        symbol = ticker['s']
        last_price = ticker['c']
        if debug:
            logger.info(f"交易对: {symbol} 最新价: {last_price}")
            pass
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

def run_ws(symbols, use_proxy=False, proxy_host=None, proxy_port=None, proxy_type=None):
    streams = '/'.join([f"{symbol.lower()}@ticker" for symbol in symbols])
    url = f"wss://fstream.binance.com/stream?streams={streams}"
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

    logger = setup_logger('binance_ticker')
    rds = redis.Redis(host=config['redis_host'], port=config['redis_port'], db=config['redis_db'])
    debug = config['debug']


    while True:
        try:
            run_ws(
                symbols=symbols,
                use_proxy=config['use_proxy'],
                proxy_host=config['proxy_host'],
                proxy_port=config['proxy_port'],
            )
        except Exception as e:
            logger.error(f"连接异常: {e}")
        logger.info("5秒后重试连接...")
        time.sleep(5)

