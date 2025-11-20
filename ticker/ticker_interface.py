import json
import time
import websocket
import redis
from typing import List, Optional
import yaml
import logging


def setup_logger(name: str):

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger


class TickerInterface:
    """
    将原脚本封装为类：
    - 初始化时配置 logger / redis / 符号列表
    - on_message / on_error / on_close / on_open 作为实例方法
    - run_ws() 建立一次连接
    - start() 提供自动重连循环
    """

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
        proxy_type: str = "socks5",  # 可按需扩展
        reconnect_delay: int = 5,
    ):
        self.exchange='exchange'
        self.debug = debug
        self.use_proxy = use_proxy
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_type = proxy_type
        self.reconnect_delay = reconnect_delay

        self.logger = setup_logger(logger_name)
        self.rds = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

        # 内部状态
        self._ws: Optional[websocket.WebSocketApp] = None
        self._closing = False

        # NEW: 运行期生效的开关与符号缓存
        self.current_exchange_a: str = ''
        self.current_exchange_b: str = ''
        self.current_symbol: List[str] = []  # 始终保持为列表

    # --------- Redis 发布 ----------
    def save_ticker_to_redis(self, symbol: str, last_price: str):
        """
        只推送ticker数据到 Redis Channel
        """
        value = json.dumps({
            "last_price": last_price,
        }, ensure_ascii=False)
        channel = f"{self.exchange}:channel:ticker:{symbol}"
        self.rds.publish(channel, value)

    # NEW: 读取运行时开关
    def get_runtime_switch(self):
        exchange_a = self.rds.get('exchange_a')
        exchange_b = self.rds.get('exchange_b')
        symbol = self.rds.get('symbol')
        exchange_a = exchange_a.decode('utf-8') if exchange_a else ''
        exchange_b = exchange_b.decode('utf-8') if exchange_b else ''
        symbol = symbol.decode('utf-8') if symbol else ''

        # 统一 symbols 为 List[str]
        symbols_list: List[str] = []
        if symbol:
            if isinstance(symbol, str):
                # 允许逗号分隔或单个
                parts = [s.strip() for s in symbol.split(',')]
                symbols_list = [p for p in parts if p]
        return exchange_a, exchange_b, symbols_list

    # --------- WebSocket 回调 ----------
    def on_message(self, ws, message: str):
        # NEW: 每次收到消息先检测配置是否变化
        try:
            new_a, new_b, new_symbols = self.get_runtime_switch()
        except Exception as e:
            self.logger.error(f"读取运行配置失败: {e}")
            new_a, new_b, new_symbols = self.current_exchange_a, self.current_exchange_b, self.current_symbol

        config_changed = (
            new_a != self.current_exchange_a or
            new_b != self.current_exchange_b or
            new_symbols != self.current_symbol
        )

        if config_changed:
            self.logger.info(
                f"检测到配置变化，将关闭WS重连。"
                f" exchange_a: {self.current_exchange_a} -> {new_a},"
                f" exchange_b: {self.current_exchange_b} -> {new_b},"
                f" symbols: {self.current_symbol} -> {new_symbols}"
            )
            try:
                self.close()
            except Exception:
                pass
            return
        self.detailed_decode_func(ws, message)

    def detailed_decode_func(self, ws, message: str):
        pass

    def on_error(self, ws, error):
        self.logger.error(f"Error: {error}")
        try:
            ws.close()
        except Exception:
            pass

    def on_close(self, ws, close_status_code, close_msg):
        self.logger.warning(f"### closed ### code={close_status_code} msg={close_msg}")

    def on_open(self, ws):
        self.logger.info("WebSocket连接已打开。")

    # --------- 建立一次连接 ----------
    def run_ws(self):
        pass

    # --------- 自动重连主循环 ----------
    def start(self):
        """
        阻塞运行，异常时等待 self.reconnect_delay 秒后重连。
        调用方若需要非阻塞，可在单独线程中运行该方法。
        """
        self._closing = False
        while not self._closing:
            try:
                self.run_ws()
            except Exception as e:
                self.logger.error(f"连接异常: {e}")
            if self._closing:
                break
            self.logger.info(f"{self.reconnect_delay}秒后重试连接...")
            time.sleep(self.reconnect_delay)

    def close(self):
        """
        主动关闭客户端（用于停止 start() 循环）
        """
        self._closing = True
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    def should_run(self):
        exchange_a, exchange_b, symbols_list = self.get_runtime_switch()

        # 是否由本客户端负责
        run_by_me = (exchange_a == self.exchange) or (exchange_b == self.exchange)

        if run_by_me:
            # 更新当前缓存（首次或切换时）
            self.current_exchange_a = exchange_a
            self.current_exchange_b = exchange_b
            self.current_symbol = symbols_list
            return True
        else:
            # 不归我管，清空符号避免误用
            self.current_exchange_a = exchange_a
            self.current_exchange_b = exchange_b
            self.current_symbol = []
            return False