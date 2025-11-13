import websocket
import json
import time
import argparse
from datetime import datetime
import redis
from utils.utils import *


WS_URL = "wss://ws.bitget.com/v2/ws/public"



def save_ticker_to_redis(rds, symbol, last_price):
    """
    只推送ticker数据到 Redis Channel（不做缓存）
    """
    value = json.dumps({
        "last_price": last_price,
    }, ensure_ascii=False)
    rds.publish(f"bitget:channel:ticker:{symbol}", value)

def build_sub_args_ticker(symbols):
    return [
        {
            "instType": "USDT-FUTURES",
            "channel": "ticker",
            "instId": symbol.upper()
        } for symbol in symbols
    ]

def on_message_ticker(ws, message):
    data = json.loads(message)
    if "action" in data and data.get("action") in ("snapshot", "update"):
        arg = data.get("arg", {})
        if arg.get("channel") == "ticker":
            inst_id = arg.get("instId")
            for t in data.get("data", []):
                last_price = t.get("lastPr")

                if debug:
                    logger.info(f"交易对: {inst_id} 最新价: {last_price}")
                save_ticker_to_redis(rds, inst_id, last_price)
    else:
        if debug:
            logger.debug(f"收到未知消息: {message}")

def on_error_ticker(ws, error):
    logger.error(f"Error: {error}")
    ws.close()

def on_close_ticker(ws, close_status_code, close_msg):
    logger.warning("### closed ###")

def on_open_ticker(ws):
    logger.info("WebSocket连接已打开。")
    sub = {
        "op": "subscribe",
        "args": build_sub_args_ticker(symbols_ticker)
    }
    ws.send(json.dumps(sub))

def run_ws_ticker(symbols_list, use_proxy=False, proxy_host=None, proxy_port=None, proxy_type=None):
    global symbols_ticker
    symbols_ticker = symbols_list
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open_ticker,
        on_message=on_message_ticker,
        on_error=on_error_ticker,
        on_close=on_close_ticker
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
    parser = argparse.ArgumentParser(description="Bitget多币种ticker WebSocket客户端")
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT', 'ETHUSDT', 'BNBUSDT'], help='交易对列表，用空格分隔，如 BTCUSDT ETHUSDT')


    args = parser.parse_args()
    symbols = load_symbols_from_yaml("symbols_list.yml")
    config = read_config('config.yml')

    logger = setup_logger('bitget_ticker')
    rds = redis.Redis(host=config['redis_host'], port=config['redis_port'], db=config['redis_db'])
    debug = config["debug"]

    while True:
        try:
            run_ws_ticker(
                symbols_list=symbols,
                use_proxy=config['use_proxy'],
                proxy_host=config['proxy_host'],
                proxy_port=config['proxy_port'],
            )
        except Exception as e:
            logger.error(f"连接异常: {e}")
        logger.info("5秒后重试连接...")
        time.sleep(5)