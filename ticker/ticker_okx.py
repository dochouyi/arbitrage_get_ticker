import json
import time
import websocket
from typing import List, Optional
import yaml
import logging

from ticker.ticker_interface import TickerInterface

def read_config(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def extract_symbol(pair_str):
    """
    从币对字符串（如 'LINK-USDT-SWAP'）中提取主币种（如 'LINK'）
    """
    # 以'-'分隔，返回第一个部分
    parts = pair_str.split('-')
    # 取前两个部分并拼接
    return ''.join(parts[:2])

def convert_symbol(symbol):
    """
    将币对名称从 ticker 格式转为 okx 格式
    例如 btcusdt -> BTC-USDT-SWAP
    """
    base = symbol[:-4].upper()
    quote = symbol[-4:].upper()
    return f"{base}-{quote}-SWAP"

class OKXTicker(TickerInterface):

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        logger_name: str = "exchange_ticker",
        debug: bool = False,
        use_proxy: bool = False,
        proxy_host: Optional[str] = None,
        proxy_port: Optional[int] = None,
        proxy_type: str = "socks5",
        reconnect_delay: int = 5,
    ):
        super().__init__(
            redis_host=redis_host,
            redis_port=redis_port,
            redis_db=redis_db,
            logger_name=logger_name,
            debug=debug,
            use_proxy=use_proxy,
            proxy_host=proxy_host,
            proxy_port=proxy_port,
            proxy_type=proxy_type,
            reconnect_delay=reconnect_delay,
        )
        self.exchange='okx'

    def detailed_decode_func(self, ws, message: str):
        # 原有消息处理逻辑
        try:
            data = json.loads(message)
        except Exception as e:
            self.logger.error(f"JSON 解析错误: {e} | 原始: {message[:200]}")
            return

        # 事件类消息
        if isinstance(data, dict) and data.get("event"):
            event = data.get("event")
            if event == "subscribe":
                self.logger.info(f"订阅成功: {data.get('arg')}")
            elif event == "error":
                self.logger.error(f"订阅错误: {data}")
            else:
                self.logger.info(f"OKX event: {data}")
            return

        # 数据消息
        if "data" in data and "arg" in data:
            for item in data["data"]:
                inst_id = item.get("instId")
                inst_id = extract_symbol(inst_id)
                last_price = item.get("last")

                if self.debug:
                    self.logger.info(f"交易对: {inst_id} 最新价: {last_price}")

                self.save_ticker_to_redis(inst_id, last_price)
        else:
            self.logger.warning(f"收到未知消息: {str(data)[:200]}")

    def on_open(self, ws):
        self.logger.info("WebSocket连接已打开。")
        sub = {
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": convert_symbol(sym)} for sym in self.current_symbol]
        }
        try:
            ws.send(json.dumps(sub))
            self.logger.info(f"已发送订阅: {sub}")
        except Exception as e:
            self.logger.error(f"发送订阅失败: {e}")

    def run_ws(self):
        # 使用当前缓存的 symbols 列表
        if not self.current_symbol:
            return

        url = "wss://ws.okx.com:8443/ws/v5/public"
        self.logger.info(f"连接URL: {url}")

        self._ws = websocket.WebSocketApp(
            url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        if self.use_proxy and self.proxy_host and self.proxy_port:
            self._ws.run_forever(
                http_proxy_host=self.proxy_host,
                http_proxy_port=self.proxy_port,
                proxy_type=self.proxy_type
            )
        else:
            self._ws.run_forever()




if __name__ == "__main__":

    config = read_config("config.yml")

    client = OKXTicker(
        redis_host=config.get('redis_host', 'localhost'),
        redis_port=config.get('redis_port', 6379),
        redis_db=config.get('redis_db', 0),
        logger_name='okx_ticker',
        debug=config.get('debug', False),
        use_proxy=config.get('use_proxy', False),
        proxy_host=config.get('proxy_host'),
        proxy_port=config.get('proxy_port'),
        proxy_type=config.get('proxy_type', 'socks5'),
        reconnect_delay=5
    )

    while True:
        try:
            if client.should_run():
                client.start()
            else:
                time.sleep(1)

        except Exception as e:
            logging.getLogger('okx_ticker_guard').error(f"主循环错误: {e}")
            time.sleep(3)