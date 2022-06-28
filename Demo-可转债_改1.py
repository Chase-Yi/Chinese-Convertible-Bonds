# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 14:48:21 2022

@author: chase 
"""

"""
自动交易程序
1，交易对象：沪深转债（剔除巳发强赎公告的转债）

2，买入
当天（T日）14点52分溢价率低于10％条件下，选取成交额最大的2只沪深可转债。
因为深市和沪市交易规则不同，深市有集合竞价，而沪市没有集合竞价，因此买入方式有所不同。

如果是深市转债（代码12开头），于14点54分到14点57分连续竟价买入帐户可用资金的25％，
15点集合竞价买入帐户可用资金的25％。

如果是沪市转债 (代码11开头)，于14点54分到15点连续竞价买入帐户可用资金的50％。

3，卖出
次日（T＋1曰）卖出，如超过开盘价0.5％自动全部卖出，
这里需要，不断的 轮巡 来查询当前价格，->  如果达到条件 再卖出。 
另外卖出以后，  还需要不断的查询 未成交订单，检查有没有成交。 
-> 没有成交的话，需要先撤单，重新发送
如没有达到，9点32分到9点33分全部卖出。
"""

#%%
# 模块0 : 导入所需数据库
import pandas as pd
import tushare as ts
import akshare as ak
import datetime

# pandas DataFrame打印输出列名对齐 (中文列名) :
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

#%%

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# TusharePro token : 49cbb8fa012ee0c2295a1b9da1b6c3a9bab7c45941579767d6daaf40

# 设置token
ts.set_token('49cbb8fa012ee0c2295a1b9da1b6c3a9bab7c45941579767d6daaf40')
  
# 初始化pro接口
pro = ts.pro_api()

# Convertible Bond : cb => 可转债

# 获取赎回信息
df = pro.cb_call(fields='ts_code,call_type,is_call,ann_date')
print('\n')
print('------------------------------')
print("债券赎回信息表")
print(df)
print('------------------------------')

#=========================================================================
# 过滤可交换债 ;  (过滤语句)
cb_df = df.ts_code.str.startswith(('12','11'))

# 过滤后的数据使用布尔选择取出来
cb_df_ = df[cb_df]

print('\n')
print('------------------------------')
print("沪深可转债: 赎回信息表")
print(cb_df_)
print('------------------------------')

#=========================================================================
# 筛选出 公告实施强赎(qs)的转债代码
qs = df[df["is_call"] == "公告实施强赎" ]

# 查重
qs.duplicated(["ts_code"])
num1 = qs.duplicated(["ts_code"]).sum()
num1 = str(num1)
print('\n')
print('------------------------------')
print("沪深可转债: 公告实施强赎表中重复的转债代码个数为: " + num1)
print('------------------------------')

if qs.duplicated(["ts_code"]).sum() == 0 :
    print('\n')
    print('------------------------------')
    print("沪深可转债: 公告实施强赎表 (无重复转债代码)")
    print(qs)
    print('------------------------------')

    # cb_df_表中，排除了在qs表中出现的结果 => cb_df_filter_1
    cb_df_filter_1 = cb_df_[~ cb_df_['ts_code'].isin(qs['ts_code'])]
    
    # 查重
    cb_df_filter_1.duplicated(["ts_code"])
    num2 = cb_df_filter_1.duplicated(["ts_code"]).sum()
    num2 = str(num2)
    print('\n')
    print('------------------------------')
    print("沪深可转债: 赎回信息表 (剔除巳发强赎公告的转债) 中重复的转债代码个数为: " + num2)
    print('------------------------------')

    if cb_df_filter_1.duplicated(["ts_code"]).sum() == 0 :
        print('\n')
        print('------------------------------')
        print("沪深可转债: 赎回信息表 (剔除巳发强赎公告的转债 + 无重复转债代码)")
        print(cb_df_filter_1)
        print('------------------------------')

        # 符合条件的交易对象所对应的转债代码
        code_0 = cb_df_filter_1['ts_code']
        # Series 转为 DataFrame
        code_df = code_0.to_frame()
        
        # 列表化
        code_list_0 = list(code_df['ts_code'])
        # 去掉列表外面的括号
        code_list = ",".join(str(i) for i in code_list_0)
        
        # 转债代码去掉 小数点和地区标识 SH,SZ
        code_1 =cb_df_filter_1['ts_code'].str[:-3]
        # code_1 转为DataFrame格式
        code_1 = pd.DataFrame(code_1)
        # 此时 code 表中的转债代码格式与AKshare接口中的转债代码格式一致

    else : 
        # 删除重复的转债代码
        cb_new_1 = cb_df_filter_1.drop_duplicates(["ts_code"])
        print('\n')
        print('------------------------------')
        print("沪深可转债: 赎回信息表 (剔除巳发强赎公告的转债 + 剔除重复转债代码)")
        print(cb_new_1)
        print('------------------------------')

        # 符合条件的交易对象所对应的转债代码
        code_0 = cb_new_1['ts_code']
        # Series 转为 DataFrame
        code_df = code_0.to_frame()
        
        # 列表化
        code_list_0 = list(code_df['ts_code'])
        # 去掉列表外面的括号
        code_list = ",".join(str(i) for i in code_list_0)
        
        # 转债代码去掉 小数点和地区标识 SH,SZ
        code_1 = cb_new_1['ts_code'].str[:-3]
        # code_1 转为DataFrame格式
        code_1 = pd.DataFrame(code_1)
        # 此时 code 表中的转债代码格式与AKshare接口中的转债代码格式一致

else :
    qs_new = qs.drop_duplicates(['ts_code'])
    print('\n')
    print('------------------------------')
    print("沪深可转债: 公告实施强赎表 (剔除重复转债代码)")
    print(qs_new)
    print('------------------------------')

    # cb_df_表中，排除了在qs_new表中出现的结果 => cb_df_filter_2
    cb_df_filter_2 = cb_df_[~ cb_df_['ts_code'].isin(qs_new['ts_code'])]
    
    # 查重
    cb_df_filter_2.duplicated(["ts_code"])
    num3 = cb_df_filter_2.duplicated(["ts_code"]).sum()
    num3 = str(num3)
    print('\n')
    print('------------------------------')
    print("沪深可转债: 赎回信息表 (剔除巳发强赎公告的转债) 中重复的转债代码个数为: " + num3)
    print('------------------------------')

    if cb_df_filter_2.duplicated(["ts_code"]).sum() == 0 :
        print('\n')
        print('------------------------------')
        print("沪深可转债: 赎回信息表 (剔除巳发强赎公告的转债 + 无重复转债代码)")
        print(cb_df_filter_2)
        print('------------------------------')

        # 符合条件的交易对象所对应的转债代码
        code_0 = cb_df_filter_2['ts_code']
        # Series 转为 DataFrame
        code_df = code_0.to_frame()
        
        # 列表化
        code_list_0 = list(code_df['ts_code'])
        # 去掉列表外面的括号
        code_list = ",".join(str(i) for i in code_list_0)
        
        # 转债代码去掉 小数点和地区标识 SH,SZ
        code_1 =cb_df_filter_2['ts_code'].str[:-3]
        # code_1 转为DataFrame格式
        code_1 = pd.DataFrame(code_1)
        # 此时 code 表中的转债代码格式与AKshare接口中的转债代码格式一致

    else : 
        # 删除重复的转债代码
        cb_new_2 = cb_df_filter_2.drop_duplicates(["ts_code"])
        print('\n')
        print('------------------------------')
        print("沪深可转债: 赎回信息表 (剔除巳发强赎公告的转债 + 剔除重复转债代码)")
        print(cb_new_2)
        print('------------------------------')

        # 符合条件的交易对象所对应的转债代码
        code_0 = cb_new_2['ts_code']
        # Series 转为 DataFrame
        code_df = code_0.to_frame()
        
        # 列表化
        code_list_0 = list(code_df['ts_code'])
        # 去掉列表外面的括号
        code_list = ",".join(str(i) for i in code_list_0)
        
        # 转债代码去掉 小数点和地区标识 SH,SZ
        code_1 = cb_new_2['ts_code'].str[:-3]
        # code_1 转为DataFrame格式
        code_1 = pd.DataFrame(code_1)
        # 此时 code 表中的转债代码格式与AKshare接口中的转债代码格式一致
            
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 模块1 : 筛选沪深转债 (剔除巳发强赎公告的转债)
class Select():
            
            def Selection(self):
                global code_1
                
                # 返回行数:
                length = code_1.shape[0]
                length = str(length)
                print('\n')
                print('------------------------------')
                print("当前筛选出的沪深可转债共有: " + length + "只")
                print('------------------------------')
                
                print('\n')
                print('------------------------------')
                print("筛选出的沪深可转债通用代码-(剔除巳发强赎公告的转债): ")
                print(code_1)
                print('------------------------------')
        
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 模块2 : 计算实时溢价率

# 可转债的溢价率一般是指转股溢价率
# 其计算公式为 溢价率 ＝ 转债价格(债现价)／转股价值 － 1
# 而转股价值的计算公式为 转股价值 ＝ 可转债面值 / 转股价 x 正股现价
# 比如，可转债面值为100，可转债的转股价为10元，对应的正股价是15元，那么转股价值＝(100／10)×15＝150元
# 溢价率＝100／150－1＝－33.33％

# 可转债对应正股的价格，就是我们最熟悉的股价 
# 把债券转成股票的转股价，也可以理解成当我们转股之后，持有股票的成本价
class on_buy():
            
            def Buy(self):
                global pro 
                global code_list
                global code_df
                global code_1
            
                time = "13:50"
            
                # 设定时间的指定格式
                ISOTIMEFORMAT = '%H:%M'
                # 使用指定格式 
                theTime = datetime.datetime.now().strftime(ISOTIMEFORMAT)
                print('\n')
                print(theTime)
                
                # 创建一个空的 DataFrame
                bond_buy = pd.DataFrame()
            
                if (theTime == time) :
                    #=================================================================
                    # 实时行情数据
                    # 接口: bond_zh_hs_cov_spot
                    # 目标地址: http://vip.stock.finance.sina.com.cn/mkt/#hskzz_z
                    # 描述: 新浪财经-沪深可转债数据
                    # 限量: 单次返回所有沪深可转债的实时行情数据
                    bond_cov_spot_df = ak.bond_zh_hs_cov_spot()
                    # bond_cov_spot_df 表中， 且在 code_1 表中出现的结果
                    bond_cov_spot_df = bond_cov_spot_df[bond_cov_spot_df['code'].isin(code_1['ts_code'])]
                    # bond_cov_spot_df.info()
                    
                    # 修改列名 
                    bond_cov_spot_df.rename(columns={'code':'ts_code','name':'bond_short_name'},inplace = True)
                    # 选取有效的列
                    bond_cov_df = bond_cov_spot_df.loc[:,['ts_code','bond_short_name','trade','volume']]
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 可转债实时债现价与成交量表 (剔除巳发强赎公告的转债)")
                    print("{Akshare与Tushare数据库中同时符合条件的沪深可转债}")
                    print('------------------------------')
                    print(bond_cov_df)
                    print('------------------------------')
                    
                    #=================================================================
                    # 可转债面值 与 对应正股代码
                    info = pro.cb_basic(fields="ts_code,bond_short_name,stk_code,stk_short_name,par")
                    
                    # info 表中， 且在 code_df 表中出现的结果
                    info_filter = info[info['ts_code'].isin(code_df['ts_code'])]
                    
                    # 转债代码去掉 小数点和地区标识 SH,SZ
                    info_filter['ts_code'] = info_filter['ts_code'].str[:-3]
                    
                    # info_filter 表中， 且在 bond_cov_spot_df 表中出现的结果
                    info_filter = info_filter[info_filter['ts_code'].isin(bond_cov_spot_df['ts_code'])]
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 对应正股代码与可转债面值表 (剔除巳发强赎公告的转债)")
                    print("{Tushare与Akshare数据库中同时符合条件的沪深可转债}")
                    print('------------------------------')
                    print(info_filter)
                    print('------------------------------')
                    
                    stock = info_filter['stk_code']
                    stock = pd.DataFrame(stock)
                    
                    # 股票代码去掉 小数点和地区标识 SH,SZ
                    stock['stk_code'] = stock['stk_code'].str[:-3]
                    
                    # 获取对应正股实时行情数据 
                    # 实时行情数据-东财
                    # 接口: stock_zh_a_spot_em
                    # 目标地址: http://quote.eastmoney.com/center/gridlist.html#hs_a_board
                    # 描述: 东方财富网-沪深京 A 股-实时行情数据
                    # 限量: 单次返回所有沪深京 A 股上市公司的实时行情数据
                    stock_spot_df = ak.stock_zh_a_spot_em()
                    
                    # stock_spot_df 表中， 且在 stock 表中出现的结果
                    stock_spot_df = stock_spot_df[stock_spot_df['代码'].isin(stock['stk_code'])]
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 对应正股实时行情表 (剔除巳发强赎公告的转债)")
                    print("{Akshare与Tushare数据库中同时符合条件的沪深可转债对应正股}")
                    print('------------------------------')
                    print(stock_spot_df)
                    print('------------------------------')
                    
                    # 备注 : 万顺转债 和 万顺转2 对应 同一个 正股 : 万顺新材
                    
                    # 备注 : 当天买入的转债，当天也可在收市之前进行转股
                    # 而在转股后的当天晚上9点以后或者第二天的早上转债就会消失，就会出现可转债的正股
                    # 而在T+1日，也就是股票第二天就可以卖出
                    
                    #=================================================================
                    # 获取可转债转股价
                    change_price_0 = pro.cb_price_chg(ts_code = code_list,fields="ts_code,bond_short_name,change_date,\
                    convert_price_initial,convertprice_bef,convertprice_aft")
                    # 去掉重复行 并保留最后一次出现的重复行 {日期格式中最接近现在的那一行}
                    change_price = change_price_0.drop_duplicates(subset=['ts_code'], keep='last')
                    # 当keep=’last’时就是保留最后一次出现的重复行。
                    
                    # 转债代码去掉 小数点和地区标识 SH,SZ
                    change_price['ts_code'] = change_price['ts_code'].str[:-3]
                    
                    # change_price 表中， 且在 bond_cov_spot_df 表中出现的结果
                    change_price = change_price[change_price['ts_code'].isin(bond_cov_spot_df['ts_code'])]
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 可转债转股价表 (剔除巳发强赎公告的转债)")
                    print("{Tushare与Akshare数据库中同时符合条件的沪深可转债}")
                    print('------------------------------')
                    print(change_price)
                    print('------------------------------')
                    
                    #=================================================================
                    # 修改列名
                    info_filter.rename(columns={'stk_short_name':'名称'},inplace = True)
                    
                    # 合并列表
                    df1 = pd.merge(stock_spot_df,info_filter,
                     how='inner',
                     left_on=['名称'],
                     right_on=['名称'])
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 对应正股实时行情与代码 + 可转债面值 (剔除巳发强赎公告的转债)")
                    print('------------------------------')
                    print(df1)
                    print('------------------------------')
                    
                    df2 = pd.merge(df1,change_price,
                     how='inner',
                     left_on=['ts_code','bond_short_name'],
                     right_on=['ts_code','bond_short_name'])
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 对应正股实时行情与代码 + 可转债面值 + 可转债转股价 (剔除巳发强赎公告的转债)")
                    print('------------------------------')
                    print(df2)
                    print('------------------------------')
                    
                    df3 = pd.merge(df2, bond_cov_df,
                     how='inner',
                     left_on=['ts_code','bond_short_name'],
                     right_on=['ts_code','bond_short_name'])
                    
                    # 查看 df3 表中所有数据中是否有NaN值 
                    df3.isnull().values.any()
                    
                    # how many missing values exist in the collection
                    df3.isnull().sum()
                    
                    # 删除 df3 表中含有任何NaN值的行
                    df4 = df3.dropna(axis=0,how='any')
                    df4.isnull().values.any()
                    
                    print('\n')
                    print('------------------------------')
                    print("沪深可转债: 对应正股实时行情与代码 + 可转债面值 + 可转债转股价 + 可转债实时债现价与成交量")
                    print("(剔除巳发强赎公告的转债与表中任何含有NaN值的行)")
                    print('------------------------------')
                    print(df4)
                    print('------------------------------')
                    
                    #=================================================================
                    # 溢价率 ＝ {债现价／转股价值} － 1
                    # 转股价值 ＝ 可转债面值 / 转股价 x 正股现价
                    
                    # df4.info()
                    
                    # 债现价 => trade
                    # 可转债面值 => par
                    # 转股价 => convertprice_aft (修正后转股价格)
                    # 正股现价 => 最新价
                    
                    # df4 表中 trade 列数据 从object转为float
                    df4['trade'] = pd.to_numeric(df4['trade'])
                    # df4.trade
                    
                    # 计算转股价值与转股溢价率 并 创建新的列
                    
                    df4["转股价值"]=df4[["par","convertprice_aft","最新价"]].apply\
                    (lambda x: x["par"] / x["convertprice_aft"] * x["最新价"],axis=1)
                    
                    df4["转股溢价率"] = df4[["trade","转股价值"]].apply\
                    (lambda x: x["trade"] / x["转股价值"] - 1,axis=1)
                    
                    # 筛选转股溢价率低于10%的沪深可转债
                    df5 = df4.loc[df4['转股溢价率'] < 0.1, ['ts_code','bond_short_name','volume','转股溢价率']]
                    print("\n")
                    print('------------------------------')
                    print("沪深可转债: 可转债实时成交量 + 可转债实时转股溢价率")
                    print("(剔除巳发强赎公告的转债 + 筛选转股溢价率低于10%的转债)")
                    print('------------------------------')
                    print(df5)
                    print('------------------------------')
                    
                    # 在溢价率低于10％条件下，选取成交额最大的2只沪深可转债
                    # 按指定列的值进行排序
                    df6 = df5.sort_values(by=['volume'])
                    
                    # DataFrame的index重新排列
                    df6.reset_index(drop=True, inplace=True)
                    
                    # 修改列名
                    df6.rename(columns={'ts_code':'可转债代码','bond_short_name':'可转债名称',\
                                        'volume':'实时成交量', '转股溢价率':'实时转股溢价率'}\
                                       ,inplace = True)
                    print("\n")
                    print('------------------------------')
                    print("沪深可转债: 可转债实时成交量 + 可转债实时转股溢价率")
                    print("(剔除巳发强赎公告的转债 + 在溢价率低于10％条件下, 按可转债实时成交量大小进行排序)")
                    print('------------------------------')
                    print(df6)
                    print('------------------------------')
                    
                    # 取最后2行 ; 
                    df7 = df6.tail(2)
                    print("\n")
                    print('------------------------------')
                    print("沪深可转债: 可转债实时成交量 + 可转债实时转股溢价率")
                    print("(剔除巳发强赎公告的转债 + 在溢价率低于10％条件下, 选取成交额最大的2只沪深可转债)")
                    print('------------------------------')
                    print(df7)
                    print('------------------------------')
                    
                    # 通过筛选的最后2只沪深可转债信息展示
                    bond1_code = df7.iat[0,0]
                    bond2_code = df7.iat[1,0]
            
                    bond1_name = df7.iat[0,1]
                    bond1_rate = df7.iat[0,3]
                    bond1_rate = "%.2f%%" % (bond1_rate * 100)
                    bond1_rate = str(bond1_rate) 
                    
                    bond2_name = df7.iat[1,1]
                    bond2_rate = df7.iat[1,3]
                    bond2_rate = "%.2f%%" % (bond2_rate * 100)
                    bond2_rate = str(bond2_rate) 
                    
                    print("\n")
                    
                    print("今日可买入的两只可转债为: " + bond1_name + "、" + bond2_name + "\n" 
                      + bond1_name + "的转股溢价率为: " + bond1_rate + " ; " +  bond1_name + "的代码为: " + bond1_code +  "\n" 
                      + bond2_name + "的转股溢价率为: " + bond2_rate + " ; " +  bond2_name + "的代码为: " + bond2_code)
                    
                    bond_buy = df7[['可转债代码', '可转债名称', '实时转股溢价率']]
                    bond_buy.reset_index(drop=True, inplace=True)
                    
                else : 
                    print("waiting")
                    pass
                
                return bond_buy
        
#%%
# 将 模块1和模块2 共同作用下 筛选出的两只可转债储存到 bond_real 中 :  

a = Select()
# 调用类Select中的函数 Selection 来展示 "符合条件的交易对象所对应的转债代码" : 
Select.Selection(a)

b = on_buy()
# 调用类A中的函数 Buy 来展示 "今日可买入的两只可转债" :
bond_real = on_buy.Buy(b)
print("\n")
print('------------------------------')
print(bond_real)

#%%
# 模块3 : 
    # 次日（T＋1曰）卖出，如超过开盘价0.5％自动全部卖出，
    # 这里需要，不断的 轮巡 来查询当前价格，->  如果达到条件 再卖出。 
    # 另外卖出以后，  还需要不断的查询 未成交订单，检查有没有成交。 
    # -> 没有成交的话，需要先撤单，重新发送
    # 如没有达到，9点32分到9点33分全部卖出。

class on_sell ():
    
    def Sell():
            
            # 设定时间的指定格式
            ISOTIMEFORMAT = '%H:%M'
            # 使用指定格式 
            SysTime = datetime.datetime.now().strftime(ISOTIMEFORMAT)
            print('\n')
            print(SysTime)
            
            time1 = "10:36"
            
            time2 = "09:32"
            time3 = "09:33"
                
            while True:
                if SysTime == time1:
                    #=========================================================
                    # 实时行情数据
                    # 接口: bond_zh_hs_cov_spot
                    # 目标地址: http://vip.stock.finance.sina.com.cn/mkt/#hskzz_z
                    # 描述: 新浪财经-沪深可转债数据
                    # 限量: 单次返回所有沪深可转债的实时行情数据
                    bond_df = ak.bond_zh_hs_cov_spot()
                    #  bond_df  表中， 且在 bond_real 表中出现的结果
                    bond_df  = bond_df [bond_df ['code'].isin(bond_real['可转债代码'])]
                
                    # 选取有效的列
                    bond_df  = bond_df .loc[:,['code','name','trade','open']]
                    # bond_df.info()
                    
                    # trade => 最新价(债现价)
                    # open => 今开(开盘价)
                    
                    # object 转 float
                    bond_df["trade"] = pd.to_numeric(bond_df["trade"])
                    bond_df["open"] = pd.to_numeric(bond_df["open"])
                    
                    bond_df["价格变化率"]= bond_df[["trade","open"]].apply\
                    (lambda x: (x["trade"] - x["open"]) / x["open"],axis=1)
                    
                    # DataFrame的index重新排列
                    bond_df.reset_index(drop=True, inplace=True)
                    
                    bond_sell_now = bond_df.loc[bond_df["价格变化率"] > 0.005]
                    bond_sell_later = bond_df.loc[bond_df["价格变化率"] <= 0.005]
                    
                    print("\n")
                    print('------------------------------')
                    print("目前可进行卖出的可转债为 : ")
                    print(bond_sell_now)
                    print('------------------------------')

                    print("\n")
                    print('------------------------------')
                    print("在9点32分到9点33分进行卖出的可转债为 : ")
                    print(bond_sell_later)
                    print('------------------------------')


