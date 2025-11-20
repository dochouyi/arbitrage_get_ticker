import collections
import numpy as np

class PriceQueue:
    def __init__(self, maxlen=100):
        self.queue = collections.deque(maxlen=maxlen)

        self.current_price_1=None
        self.current_price_2=None
        self.mean=None
        self.std=None
        self.std_times=1.5

    def put_prices(self, price_1,price_2):
        self.current_price_1 = price_1
        self.current_price_2 = price_2
        rate=(price_1-price_2)/price_2
        self.queue.append(float(rate))

        if len(self.queue)<10:
            return None,None
        scope_index=self.calcu_scope(rate)

        if self.mean<0 and scope_index==1:
            return 'in','longA_shortB'
        if self.mean>0 and scope_index==2:
            return 'in','shortA_longB'

        if self.mean<0 and scope_index==2:
            return 'out','shortA_longB'
        if self.mean>0 and scope_index==1:
            return 'out','longA_shortB'
        return None,None

    #是否在1.5标准差范围内
    def calcu_scope(self, rate):
        mean, std=self.gaussian_fit()
        self.mean=mean
        self.std=std
        if (rate<mean-std*self.std_times):
            return 1
        elif(rate>mean+std*self.std_times):
            return 2
        else:
            return 0

    def gaussian_fit(self):
        """
        返回队列中所有数据的高斯分布拟合均值和标准差
        :return: (mean, std)
        """
        if not self.queue:
            return None, None
        data = np.array(self.queue)
        mean = np.mean(data)
        std = np.std(data, ddof=0)  # ddof=0对应总体标准差
        return mean, std


