# -*- coding: utf-8 -*-
"""
交易日检查工具
"""

import pandas as pd
from typing import Optional

try:
    import exchange_calendars as xcals
    # 获取上海证券交易所日历（代表A股交易日历）
    # 指定 start="2000-01-01" 以支持 2000 年之后的数据
    XSHG_CALENDAR = xcals.get_calendar("XSHG", start="2000-01-01")
    HAS_EXCHANGE_CALENDAR = True
except (ImportError, Exception):
    XSHG_CALENDAR = None
    HAS_EXCHANGE_CALENDAR = False


def has_trading_day(start_date: str, end_date: str) -> bool:
    """
    判断日期范围内是否有A股交易日
    
    优先使用 exchange_calendars 库（专业的交易所日历，准确识别A股交易日）
    如果不可用，则降级为简单的周末判断
    
    注意：
        - 日历配置为从 2000-01-01 开始
        - 支持范围：2000-01-04 至 2026-12-31
        - 2000 年之前的数据会自动降级为周末判断（周一到周五 = 交易日）
    
    Args:
        start_date (str): 开始日期，格式 'YYYYMMDD'
        end_date (str): 结束日期，格式 'YYYYMMDD'
    
    Returns:
        bool: 如果日期范围内有交易日返回 True，全是非交易日返回 False
    
    Examples:
        >>> has_trading_day('20000101', '20000102')  # 2000-01-01 是周六，2000-01-02 是周日
        False
        >>> has_trading_day('20000104', '20000105')  # 2000-01-04 是周二，2000-01-05 是周三
        True
        >>> has_trading_day('20240210', '20240217')  # 2024年春节假期
        False
        >>> has_trading_day('20240218', '20240218')  # 2024-02-18 周日调休工作日，但股市不开
        False
    """
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # 生成日期范围
        date_range = pd.date_range(start=start, end=end, freq='D')
        
        # 优先使用 exchange_calendars（最准确）
        if HAS_EXCHANGE_CALENDAR and XSHG_CALENDAR is not None:
            try:
                # 检查是否有任何交易日
                has_session = any(XSHG_CALENDAR.is_session(d) for d in date_range)
                return has_session
            except Exception:
                # 如果查询失败，降级处理
                pass
        
        # 降级方案：检查是否有非周末的日期（周一到周五，weekday < 5）
        has_weekday = any(d.weekday() < 5 for d in date_range)
        
        return has_weekday
    except Exception:
        return True  # 异常情况下，保守地认为有交易日，尝试获取数据
