import numpy as np
from atrader import *
from sympy import *
import matplotlib.pyplot as plt
from scipy.spatial.distance import euclidean
from fastdtw import fastdtw

def init(context):
    set_backtest(initial_cash=10000000)
    reg_kdata('min',1)
    context.Tlen=len(context.target_list)  #标的个数
    context.initial=10000000
    context.buy_price=-1
    context.data = get_kdata('sse.000300', 'day', 1, begin_date='2011-01-01', end_date='2017-01-01', fq=1, fill_up=False,
                     df=False, sort_by_date=True)
    context.data = context.data['sse.000300']
    context.data = context.data[['time', 'close','volume','open']]

    #计算收益率
    context.data['rate'] = (context.data['close'] - context.data['close'].shift(1)) / context.data['close'].shift(1)
    #计算日内收益率
    context.data['day_rate'] = context.data['close']/context.data['open'] - 1
    #计算volume变化率
    context.data['volume_rate'] = (context.data['volume'] - context.data['volume'].shift(1)) / context.data['volume'].shift(1)
    #购买价
    context.buy_price=-1
    #行情序列长度
    context.L=8
    context.L1 = 10
    context.L2 = 11
    #窗口长度
    context.N = context.L
    context.N1 = context.L1
    context.N2 = context.L2
    #匹配后选取排名
    context.K = 10
    #获取回测开始日期的位置
    for i in range(1, len(context.data)):
        if str(context.data.iloc[i][0]) > begin and str(context.data.iloc[i-1][0]) < begin:
            context.day=i-1
    context.origin = context.day
    #记录distance
    context.jilu=pd.DataFrame()
    position = context.day

    #记录distance，并写到csv文件中
    for j in range(context.day+1,len(context.data)):
        position = position + 1
        distance = np.empty((position-context.L-1,2))
        price_std_x = np.std(context.data['rate'][position-context.L:position])
        volume_std_x = np.std(context.data['volume_rate'][position-context.L:position])
        for idx in range(1, position - context.L):
            compare = context.data[['volume_rate', 'rate']][idx:idx + context.L]
            price_std_y = np.std(compare['rate'])
            volume_std_y = np.std(compare['volume_rate'])
            time_series_A = list(zip(context.data['rate'][position-context.L:position] / price_std_x, context.data['volume_rate'][position-context.L:position] / volume_std_x))
            time_series_B = list(zip(compare['rate'] / price_std_y, compare['volume_rate'] / volume_std_y))
            distance[idx-1][1], path = fastdtw(time_series_A, time_series_B, dist=euclidean)
            distance[idx-1][0] = idx
        distance_arg = np.argsort(distance[:, 1])
        distance = distance[distance_arg].tolist()
        distance = pd.DataFrame(distance)
        context.jilu = pd.concat([context.jilu,distance],axis=1)
        print(j)
    context.jilu.to_csv('D:\python_file\L12.csv')

    # 读取distance的excel文件
    context.record = pd.read_csv('D:\python_file\L8.csv')
    context.record1 = pd.read_csv('D:\python_file\L10.csv')
    context.record2 = pd.read_csv('D:\python_file\L11.csv')
    context.record = context.record.drop(['Unnamed: 0'],axis=1)
    context.record1 = context.record1.drop(['Unnamed: 0'], axis=1)
    context.record2 = context.record2.drop(['Unnamed: 0'], axis=1)

def on_data(context):
    long_positions = context.account().positions['volume_long']
    short_positions = context.account().positions['volume_short']
    dt = get_reg_kdata(reg_idx=context.reg_kdata[0], length=context.N, fill_up=True, df=True)
    cangwei = 0
    if context.day_begin :
        dt = dt[['time', 'close', 'volume']]
        context.day = context.day + 1
        #记录建仓仓位

        weight = np.empty(context.K)
        fenmu, fenzi = 0, 0
        #选取排名在前K个的进行加权
        for i in range(0, context.K):
            weight[i] = 1 / context.record.iloc[i][2*(context.day-context.origin)-1]
            fenzi = fenzi + weight[i] * context.data['day_rate'][context.record.iloc[i][2*(context.day-context.origin-1)] + context.L]
            fenmu = fenmu + weight[i]
        ret_time = fenzi / fenmu
        if ret_time > 0.0005:
            cangwei = cangwei +1
        if ret_time < -0.0005:
            cangwei = cangwei - 1

        weight = np.empty(context.K)
        fenmu, fenzi = 0, 0
        #选取排名在前K个的进行加权
        for i in range(0, context.K):
            weight[i] = 1 / context.record1.iloc[i][2*(context.day-context.origin)-1]
            fenzi = fenzi + weight[i] * context.data['day_rate'][context.record1.iloc[i][2*(context.day-context.origin-1)] + context.L]
            fenmu = fenmu + weight[i]
        ret_time1 = fenzi / fenmu
        if ret_time1 > 0.0005:
            cangwei = cangwei +1
        if ret_time1 < -0.0005:
            cangwei = cangwei - 1

        '''
        weight = np.empty(context.K)
        fenmu, fenzi = 0, 0
        #选取排名在前K个的进行加权
        for i in range(0, context.K):
            weight[i] = 1 / context.record2.iloc[i][2*(context.day-context.origin)-1]
            fenzi = fenzi + weight[i] * context.data['day_rate'][context.record2.iloc[i][2*(context.day-context.origin-1)] + context.L]
            fenmu = fenmu + weight[i]
        ret_time2 = fenzi / fenmu

        if ret_time2 > 0.0005:
            cangwei = cangwei +1

        if ret_time2 < -0.0005:
            cangwei = cangwei - 1
        '''

        # 设置多头，空头
        long_open = cangwei > 0 and long_positions[0] == 0
        short_open = cangwei < 0 and short_positions[0] == 0


        if cangwei ==0:
            order_target_value(account_idx=0, target_idx=0, target_value=0,
                               side=1, order_type=2)
            print('0仓')

        if long_open:
            if cangwei==1:
               order_target_value(account_idx=0, target_idx=0, target_value=context.initial,
                               side=1, order_type=2)
               print('开多')
            if cangwei==2:
               order_target_value(account_idx=0, target_idx=0, target_value=2*context.initial,
                               side=1, order_type=2)
               print('两倍开多')
            '''
            if cangwei == 3:
                order_target_value(account_idx=0, target_idx=0, target_value=2*context.initial,
                                   side=1, order_type=2)
                print('两倍开多')
            '''
            context.buy_price = dt['close'][context.N - 1]

        if short_open:
            if cangwei == -1:
                order_target_value(account_idx=0, target_idx=0, target_value=context.initial,
                               side=2, order_type=2)
                print('开空')

            if cangwei == -2:
               order_target_value(account_idx=0, target_idx=0, target_value=2*context.initial,
                               side=2, order_type=2)
               print('两倍开空')
            '''
            if cangwei == -3:
                order_target_value(account_idx=0, target_idx=0, target_value=2*context.initial,
                               side=2, order_type=2)
                print('两倍开空')
            '''
            context.buy_price = dt['close'][context.N - 1]
    '''
    #日内平仓
    if str(1459) == context.now.strftime('%H%M'):
        order_target_volume(account_idx=0, target_idx=0, target_volume=0, side=1, order_type=2)
        context.buy_price = -1
    if context.now.strftime('%H%M') == '1500':
        return
    '''

    # 设置平仓条件
    # 多头止损
    long_close = dt['close'][context.N-1] / context.buy_price < 0.99 and long_positions[0] > 0 and context.buy_price > 0
    # 空头止损
    short_close = dt['close'][context.N-1] / context.buy_price > 1.01 and short_positions[0] > 0 and context.buy_price > 0

    if long_close :
         order_target_volume(account_idx=0, target_idx=0, target_volume=0, side=1, order_type=2)
         context.buy_price = -1
         print('多头止损')

    elif short_close  :
         order_target_volume(account_idx=0, target_idx=0, target_volume=0, side=1, order_type=2)
         context.buy_price = -1
         print('空头止损')


global begin,end
begin = '2013-01-01'
end = '2017-01-01'

if __name__=='__main__':
    target = ['cffex.if0000']
    id=run_backtest('DTWStrategy', '.', target_list=target, frequency='min', fre_num=1,
                 begin_date=begin, end_date=end, fq=1)