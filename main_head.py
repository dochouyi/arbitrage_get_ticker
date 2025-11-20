import redis
import threading
import time
import json
import collections
import signal
import sys
import math

import matplotlib
matplotlib.use('tkagg')  # 可改为 'qt5agg' 或 'agg'（无界面）
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from utils.data_queue import PriceQueue


class DualOscilloscopePlotter:
    """
    一个窗口内绘制两条实时曲线（上下两个子图，等高）：
    - 上图：价差（A - B）
    - 下图：价差百分比（默认相对 B： (A - B)/B * 100）
    - x 轴为相对时间（秒），固定 [-window, 0]
    """
    def __init__(self, window_seconds=300, fps=25,
                 title_top="Spread A - B", title_bottom="Spread% vs B",
                 y_label_top="Spread", y_label_bottom="Spread (%)",
                 color_top='lime', color_bottom='deepskyblue'):
        self.window_seconds = window_seconds
        self.fps = fps

        # 两条曲线的缓冲区
        self.buf_top = collections.deque(maxlen=10_000)     # (ts, value) for spread
        self.buf_bottom = collections.deque(maxlen=10_000)  # (ts, value) for spread%
        self.lock = threading.Lock()

        # 图形与子图（等高）
        self.fig, (self.ax_top, self.ax_bottom) = plt.subplots(
            2, 1, figsize=(15, 8), sharex=True, gridspec_kw={'height_ratios': [1, 1]}
        )
        self.fig.patch.set_facecolor('#0c0f12')

        # 上图
        self.line_top, = self.ax_top.plot([], [], color=color_top, linewidth=1.5)
        self._style_axis(self.ax_top, title_top, "Time (s)", y_label_top)

        # 下图
        self.line_bottom, = self.ax_bottom.plot([], [], color=color_bottom, linewidth=1.5)
        self._style_axis(self.ax_bottom, title_bottom, "Time (s)", y_label_bottom)

        plt.tight_layout()

        self.anim = None
        self._running = False

        # y 轴自适应参数（分别独立）
        self._last_ylim_update_top = 0
        self._last_ylim_update_bottom = 0
        self._ylim_cushion = 0.1
        self._ylim_min_refresh = 0.2

    def _style_axis(self, ax, title, xlabel, ylabel):
        ax.set_facecolor('#0c0f12')
        ax.grid(True, color='#2a2f36', linestyle='--', linewidth=0.6)
        ax.set_title(title, color='white')
        ax.set_xlabel(xlabel, color='white')
        ax.set_ylabel(ylabel, color='white')
        for spine in ax.spines.values():
            spine.set_color('#3a4048')
        ax.tick_params(colors='white')

    def add_point_top(self, value, ts=None):
        if ts is None:
            ts = time.time()
        with self.lock:
            self.buf_top.append((ts, value))

    def add_point_bottom(self, value, ts=None):
        if ts is None:
            ts = time.time()
        with self.lock:
            self.buf_bottom.append((ts, value))

    def _get_windowed(self, buf):
        now = time.time()
        left = now - self.window_seconds
        xs, ys = [], []
        with self.lock:
            for t, v in buf:
                if t >= left:
                    xs.append(t)
                    ys.append(v)
        xs = [x - now for x in xs]  # 相对时间：负值在左
        return xs, ys

    def _update_ylim_axis(self, ax, ys, is_top=True):
        if not ys:
            return
        now = time.time()
        last_update = self._last_ylim_update_top if is_top else self._last_ylim_update_bottom
        if now - last_update < self._ylim_min_refresh:
            return
        ymin, ymax = min(ys), max(ys)
        if math.isclose(ymin, ymax):
            ymin -= 1
            ymax += 1
        span = ymax - ymin
        ymin -= span * self._ylim_cushion
        ymax += span * self._ylim_cushion
        ax.set_ylim(ymin, ymax)
        if is_top:
            self._last_ylim_update_top = now
        else:
            self._last_ylim_update_bottom = now

    def _update(self, _frame):
        # 上图数据
        xs_top, ys_top = self._get_windowed(self.buf_top)
        self.line_top.set_data(xs_top, ys_top)
        self.ax_top.set_xlim(-self.window_seconds, 0)
        self._update_ylim_axis(self.ax_top, ys_top, is_top=True)

        # 下图数据
        xs_bottom, ys_bottom = self._get_windowed(self.buf_bottom)
        self.line_bottom.set_data(xs_bottom, ys_bottom)
        self.ax_bottom.set_xlim(-self.window_seconds, 0)
        self._update_ylim_axis(self.ax_bottom, ys_bottom, is_top=False)

        return self.line_top, self.line_bottom

    def start(self, block=False):
        if self._running:
            return
        self._running = True
        interval_ms = 1000 / self.fps
        self.anim = FuncAnimation(self.fig, self._update, interval=interval_ms, blit=True)
        plt.show(block=block)

    def stop(self):
        self._running = False
        plt.close(self.fig)


class RedisTickerListener:
    def __init__(self, exchange_1="binance", exchange_2="bybit", symbol="BTCUSDT", host='localhost', port=6379, db=0):
        self.exchange_name_list = ['binance', 'bybit', 'okx', 'bitget']

        self.redis = redis.Redis(host=host, port=port, db=db)
        self.channels = [f'{exchange_1}:channel:ticker:{symbol}', f'{exchange_2}:channel:ticker:{symbol}']
        self.publish_command(exchange_1, exchange_2, symbol)
        self.latest_data = {}
        self.lock = threading.Lock()
        self._stop_event = threading.Event()

        # 单窗口（等高）双曲线绘图器
        self.plotter = DualOscilloscopePlotter(
            window_seconds=300, fps=25,
            title_top=f"Spread {exchange_1} - {exchange_2} ({symbol})",
            title_bottom=f"Spread% {exchange_1} vs {exchange_2} ({symbol})",
            y_label_top="Spread", y_label_bottom="Spread (%)",
            color_top='lime', color_bottom='deepskyblue'
        )

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

    def print_and_plot_latest(self):
        """
        每秒打印一次两个通道的最新值，并更新同一图中的两条曲线。
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

            # print(output)
            self.prev_output = output.copy()

            a = output.get(ch_a)
            b = output.get(ch_b)

            if isinstance(a, (int, float)) and isinstance(b, (int, float)) and a is not None and b is not None:

                decision = self.price_queue.put_prices(a, b)
                if decision != (None, None):
                    print(f"Decision: {decision}, a: {a}, b: {b}")

                spread = a - b
                self.plotter.add_point_top(spread)

                if b != 0:
                    spread_pct = (a - b) / b * 100.0
                else:
                    spread_pct = 0.0
                self.plotter.add_point_bottom(spread_pct)

    def start(self):
        self._stop_event.clear()
        # 主线程启动 UI
        self.plotter.start(block=False)

        # 后台线程
        self.t_listen = threading.Thread(target=self.listen_redis, name="redis-listener", daemon=True)
        self.t_print = threading.Thread(target=self.print_and_plot_latest, name="printer-plotter", daemon=True)
        self.t_listen.start()
        self.t_print.start()

    def stop(self):
        self._stop_event.set()
        self.plotter.stop()

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
                plt.pause(0.05)
        except KeyboardInterrupt:
            print("Stopping...")
            self.stop()


if __name__ == "__main__":
    listener = RedisTickerListener(exchange_1="binance", exchange_2="bybit", symbol="COAIUSDT")
    listener.run_forever()
