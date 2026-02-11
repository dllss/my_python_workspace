#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试市场状态检测功能
演示如何检测当前是否适合获取股票数据
"""

from datetime import datetime, timedelta
from utils import check_market_status

# 示例：不同时间的状态
print("交易时间说明：")
print("  上午盘：09:30 - 11:30")
print("  午休：  11:30 - 13:00")
print("  下午盘：13:00 - 15:00")
print("  收盘：  15:00")
print()

result = check_market_status()

print("推荐配置：")
if result:
    print("  END_DATE = datetime.now().strftime('%Y%m%d')  # 今天")
else:
    print("  END_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')  # 昨天")
print()

print("说明：")
print("  🟢 绿色 = 适合获取今日数据")
print("  🟡 黄色 = 谨慎，数据可能不完整")
print("  🔴 红色 = 休市，无法获取今日数据")
