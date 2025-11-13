import redis
import threading
import time
import json

class RedisTickerListener:
    def __init__(self, exchange_1="binance",exchange_2="bybit", symbol="BTCUSDT", host='localhost', port=6379, db=0):
        self.exchange_name_list=['binance','bybit','okx','bitget']

        self.redis = redis.Redis(host=host, port=port, db=db)
        self.channels = [f'{exchange_1}:channel:ticker:{symbol}', f'{exchange_2}:channel:ticker:{symbol}']
        self.publish_command(exchange_1,exchange_2,symbol)
        self.latest_data = {}
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

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
                for ch in self.channels:
                    if ch in channel:
                        data = json.loads(message['data'])['last_price']
                        with self.lock:
                            self.latest_data[ch] = data

    def print_latest(self):

        prev_output = {ch: None for ch in self.channels}
        while not self._stop_event.is_set():
            time.sleep(1)
            output = {}
            with self.lock:
                for ch in self.channels:
                    output[ch] = self.latest_data.get(ch, prev_output[ch])
                self.latest_data.clear()
            print(output)
            prev_output = output.copy()

    def start(self):
        self._stop_event.clear()
        self.t1 = threading.Thread(target=self.listen_redis)
        self.t2 = threading.Thread(target=self.print_latest)
        self.t1.daemon = True
        self.t2.daemon = True
        self.t1.start()
        self.t2.start()

    def stop(self):
        self._stop_event.set()
        # 线程为守护线程，主线程退出时会自动结束

    def run_forever(self):

        self.start()
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            print("Stopping...")
            self.stop()

if __name__ == "__main__":
    listener = RedisTickerListener()
    listener.run_forever()
