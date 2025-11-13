import websocket
import json
import time
import argparse
import logging
from datetime import datetime
import redis
from utils.utils import *

def extract_symbol(pair_str):
    """
    从币对字符串（如 'LINK-USDT-SWAP'）中提取主币种（如 'LINK'）
    """
    # 以'-'分隔，返回第一个部分
    parts = pair_str.split('-')
    # 取前两个部分并拼接
    return ''.join(parts[:2])


def save_ticker_to_redis(rds, symbol, last_price):
    """
    只推送ticker数据到 Redis Channel（不做缓存）
    """
    value = json.dumps({
        "last_price": last_price,
    }, ensure_ascii=False)
    rds.publish(f"okx:channel:ticker:{symbol}", value)

def on_message(ws, message):
    try:
        data = json.loads(message)
    except Exception as e:
        logger.error(f"JSON 解析错误: {e} | 原始: {message[:200]}")
        return

    # 事件类消息
    if isinstance(data, dict) and data.get("event"):
        event = data.get("event")
        if event == "subscribe":
            logger.info(f"订阅成功: {data.get('arg')}")
        elif event == "error":
            logger.error(f"订阅错误: {data}")
        else:
            logger.info(f"OKX event: {data}")
        return

    # 数据消息
    if "data" in data and "arg" in data:
        for item in data["data"]:
            inst_id = item.get("instId")
            inst_id=extract_symbol(inst_id)
            last_price = item.get("last")

            if debug:
                logger.info(f"交易对: {inst_id} 最新价: {last_price}")

            save_ticker_to_redis(rds, inst_id, last_price)
    else:
        logger.warning(f"收到未知消息: {str(data)[:200]}")

def on_error(ws, error):
    logger.error(f"Error: {error}")
    try:
        ws.close()
    except Exception:
        pass

def on_close(ws, close_status_code, close_msg):
    logger.warning("### closed ###")

def on_open(ws):
    logger.info("WebSocket连接已打开。")

def run_ws(symbols, use_proxy=False, proxy_host=None, proxy_port=None, proxy_type=None):
    url = "wss://ws.okx.com:8443/ws/v5/public"
    ws = websocket.WebSocketApp(
        url,
        on_open=lambda w: on_open_and_subscribe(w, symbols),
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
        ws.run_forever(origin="https://www.okx.com", ping_interval=20, ping_timeout=10)

def on_open_and_subscribe(ws, symbols):
    on_open(ws)
    sub = {
        "op": "subscribe",
        "args": [{"channel": "tickers", "instId": sym} for sym in symbols]
    }
    try:
        ws.send(json.dumps(sub))
        logger.info(f"已发送订阅: {sub}")
    except Exception as e:
        logger.error(f"发送订阅失败: {e}")


if __name__ == "__main__":
    symbols_ = load_symbols_from_yaml("symbols_list.yml")
    symbols = [convert_symbol(s) for s in symbols_]
    config = read_config('config.yml')
    logger = setup_logger('okx_ticker')

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

# 示例：
# python ticker.py --symbols BTC-USDT-SWAP ETH-USDT-SWAP --use_proxy --proxy_host 127.0.0.1 --proxy_port 7891 --debug