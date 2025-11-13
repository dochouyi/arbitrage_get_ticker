import redis
import subprocess
import yaml
import time

class RedisDockerMonitor:
    def __init__(self, redis_host='localhost', redis_port=6379, redis_db=0, check_interval=5):

        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        self.check_interval = check_interval
        self.last_exchange_a = None
        self.last_exchange_b = None
        self.last_symbol = None

    def run_docker_compose(self,exchange_a,exchange_b):

        sub_cmds=[]
        if self.last_exchange_a!=None:
            cmd= ["docker", "compose", "-f", f"docker_compose_ticker_{self.last_exchange_a}.yml", "down"]
            sub_cmds.append(cmd)
        if self.last_exchange_b != None:
            cmd = ["docker", "compose", "-f", f"docker_compose_ticker_{self.last_exchange_b}.yml", "down"]
            sub_cmds.append(cmd)


        time.sleep(3)
        for cmd in sub_cmds:
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                print(f"执行失败：{cmd}，错误：{e}")

        cmd1=["docker", "compose", "-f", f"docker_compose_ticker_{exchange_a}.yml", "up", "-d"]
        cmd2=["docker", "compose", "-f", f"docker_compose_ticker_{exchange_b}.yml", "up", "-d"]

        sub_cmds=[cmd1,cmd2]
        for cmd in sub_cmds:
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                print(f"执行失败：{cmd}，错误：{e}")
    def stop_all_containers(self):
        cmds=[
            ["docker", "compose", "-f", f"docker_compose_ticker_binance.yml", "down"],
            ["docker", "compose", "-f", f"docker_compose_ticker_bitget.yml", "down"],
            ["docker", "compose", "-f", f"docker_compose_ticker_bybit.yml", "down"],
            ["docker", "compose", "-f", f"docker_compose_ticker_okx.yml", "down"]
        ]
        for cmd in cmds:
            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                print(f"执行失败：{cmd}，错误：{e}")

    def write_symbol_to_yaml(self, file_path, symbol):
        """
        直接用新的symbol覆盖写入symbols_list.yml文件
        :param file_path: yaml文件路径
        :param symbol: 新的symbol字符串
        """

        data = {'symbols': [symbol]}
        with open(file_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True)

    def process_command(self, exchange_a, exchange_b, symbol):
        print(f"处理新命令: exchange_a={exchange_a}, exchange_b={exchange_b}, symbol={symbol}")
        # 使用示例
        self.write_symbol_to_yaml('symbols_list.yml', symbol)
        self.run_docker_compose(exchange_a,exchange_b)

    def monitor_redis_command(self):
        self.stop_all_containers()

        while True:

            exchange_a = self.redis_client.get('exchange_a')
            exchange_b = self.redis_client.get('exchange_b')
            symbol = self.redis_client.get('symbol')
            # 解码为字符串
            exchange_a = exchange_a.decode('utf-8') if exchange_a else ''
            exchange_b = exchange_b.decode('utf-8') if exchange_b else ''
            symbol = symbol.decode('utf-8') if symbol else ''
            # 检查是否有变化
            if (exchange_a != self.last_exchange_a or
                exchange_b != self.last_exchange_b or
                symbol != self.last_symbol):
                self.last_symbol = symbol
                self.process_command(exchange_a, exchange_b, symbol)
                self.last_exchange_a = exchange_a
                self.last_exchange_b = exchange_b
            time.sleep(self.check_interval)


if __name__ == "__main__":
    monitor = RedisDockerMonitor()
    monitor.monitor_redis_command()
