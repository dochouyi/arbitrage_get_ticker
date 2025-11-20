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

class BybitTicker(TickerInterface):

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
        self.exchange='bybit'
        self.previous_last_price=None

    def detailed_decode_func(self, ws, message: str):
        data = json.loads(message)
        topic = data.get("topic", "")
        if topic.startswith("tickers."):
            ticker = data.get("data", {})
            symbol = ticker.get("symbol") or ticker.get("s")
            last_price = ticker.get("lastPrice") or ticker.get("last_price") or ticker.get("lp")
            if last_price == None:
                last_price = self.previous_last_price
            else:
                self.previous_last_price = last_price
            if self.debug:
                self.logger.info(f"交易对: {symbol} 最新价: {last_price}")
            self.save_ticker_to_redis(symbol, last_price)
        else:
            self.logger.warning(f"收到未知消息: {message}")

    def on_open(self, ws):
        self.logger.info("WebSocket连接已打开。")
        args = [f"tickers.{sym.upper()}" for sym in self.current_symbol]
        sub_msg = {"op": "subscribe", "args": args}
        self._ws.send(json.dumps(sub_msg))
        self.logger.info(f"已订阅: {args}")

    def run_ws(self):
        # 使用当前缓存的 symbols 列表
        if not self.current_symbol:
            return

        url = "wss://stream.bybit.com/v5/public/linear"
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

    client = BybitTicker(
        redis_host=config.get('redis_host', 'localhost'),
        redis_port=config.get('redis_port', 6379),
        redis_db=config.get('redis_db', 0),
        logger_name='bybit_ticker',
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
            logging.getLogger('bybit_ticker_guard').error(f"主循环错误: {e}")
            time.sleep(3)