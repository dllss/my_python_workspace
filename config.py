# -*- coding: utf-8 -*-
"""
配置文件
集中管理项目中的常量和配置参数
"""

from datetime import datetime, timedelta

# ========== 文件路径配置 ==========
OUTPUT_DIR = "data"                 # 数据输出目录
CN_DIR = "CN"                       # 中国A股数据子目录

# ========== 股票数据配置 ==========
STOCK_CODE = "600519"               # 默认股票代码（贵州茅台）
START_DATE = "20000101"             # 开始日期
END_DATE = "20260210"               # 结束日期
# END_DATE = datetime.now().strftime("%Y%m%d")  # 结束日期（今天）
# END_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")  # 结束日期（昨天）
ADJUST_TYPE = "qfq"                 # 复权类型: qfq(前复权), hfq(后复权), ""(不复权)

# ========== 批量获取配置 ==========
DELAY_MIN = 2                       # 最小延迟（秒）
DELAY_MAX = 4                       # 最大延迟（秒）
BATCH_SIZE = 0                   # 每批处理的股票数量（0表示处理全部）
START_INDEX = 0                     # 从第几只股票开始（0表示从头开始）
UPDATE_MODE = "tail"                # 更新模式: tail(只补充尾部), full(完全刷新), head_tail(补充头尾)

# ========== 数据源配置 ==========
PREFERRED_SOURCE = "baostock"       # 优先数据源: baostock(默认), akshare, yfinance