import redis
import threading
import time
import json
import collections
import signal
import sys
import math

from utils.data_queue import PriceQueue

class RedisTickerListener:
    def __init__(self, exchange_1="binance", exchange_2="bybit", symbol="BTCUSDT", host='localhost', port=6379, db=0):
        self.exchange_name_list = ['binance', 'bybit', 'okx', 'bitget']

        self.redis = redis.Redis(host=host, port=port, db=db)
        self.channels = [f'{exchange_1}:channel:ticker:{symbol}', f'{exchange_2}:channel:ticker:{symbol}']
        self.publish_command(exchange_1, exchange_2, symbol)
        self.latest_data = {}
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

        # 用于计算的上一时刻值
        self.prev_output = {ch: None for ch in self.channels}
        self.price_queue=PriceQueue()

    def publish_command(self, exchange_a, exchange_b, symbol):
        self.redis.set('exchange_a', exchange_a)
        self.redis.set('exchange_b', exchange_b)
        self.redis.set('symbol', symbol)
        print(f"已发布: exchange_a={exchange_a}, exchange_b={exchange_b}, symbol={symbol}")

    def listen_redis(self):
        pattern = '*:channel:ticker:*'
        pubsub = self.redis.pubsub()
        pubsub.psubscribe(pattern)

        for message in pubsub.listen():
            if self._stop_event.is_set():
                break
            if message['type'] == 'pmessage':
                channel = message['channel'].decode()
                # 只接收目标两个通道
                for ch in self.channels:
                    if ch in channel:
                        try:
                            payload = message['data']
                            payload = payload.decode('utf-8')
                            data_json = json.loads(payload)

                            data = float(data_json['last_price'])

                            with self.lock:
                                self.latest_data[ch] = data
                        except Exception as e:
                            print(f"[listen_redis] 解析消息异常: {e}")

    def print_latest(self):
        """
        每秒打印一次两个通道的最新值，并计算价差和百分比。
        """
        self.prev_output = {ch: None for ch in self.channels}
        ch_a, ch_b = self.channels

        while not self._stop_event.is_set():
            time.sleep(1)
            output = {}
            with self.lock:
                for ch in self.channels:
                    output[ch] = self.latest_data.get(ch, self.prev_output[ch])
                self.latest_data.clear()

            self.prev_output = output.copy()

            a = output.get(ch_a)
            b = output.get(ch_b)

            if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a is not None and b is not None:

                decision = self.price_queue.put_prices(a, b)
                if decision != (None, None):
                    print(f"Decision: {decision}, a: {a}, b: {b}")

    def start(self):
        self._stop_event.clear()
        # 后台线程
        self.t_listen = threading.Thread(target=self.listen_redis, name="redis-listener", daemon=True)
        self.t_print = threading.Thread(target=self.print_latest, name="printer", daemon=True)
        self.t_listen.start()
        self.t_print.start()

    def stop(self):
        self._stop_event.set()

    def run_forever(self):
        self.start()

        def _sigint_handler(signum, frame):
            print("Stopping...")
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, _sigint_handler)
        signal.signal(signal.SIGTERM, _sigint_handler)

        try:
            while not self._stop_event.is_set():
                time.sleep(0.05)
        except KeyboardInterrupt:
            print("Stopping...")
            self.stop()

if __name__ == "__main__":
    listener = RedisTickerListener(exchange_1="binance", exchange_2="bybit", symbol="GRASSUSDT")
    listener.run_forever()
