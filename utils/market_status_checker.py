# -*- coding: utf-8 -*-
"""
市场状态检查工具
提供安全的结束日期检查功能（18:30 之前强制使用昨天）
"""

from datetime import datetime, timedelta
from typing import Tuple


def get_safe_end_date(end_date_str: str) -> Tuple[str, bool]:
    """
    获取安全的结束日期（18:30之前强制使用昨天）
    
    Args:
        end_date_str: 用户配置的结束日期，格式 'YYYYMMDD'
    
    Returns:
        Tuple[safe_end_date, date_adjusted]:
            - safe_end_date: 安全的结束日期（18:30之前会调整为昨天）
            - date_adjusted: 是否调整了日期
    
    Examples:
        >>> # 18:30 之前运行，配置今天
        >>> get_safe_end_date('20260211')  # 假设现在是 17:00
        ('20260210', True)  # 自动调整为昨天
        
        >>> # 18:30 之后运行，配置今天
        >>> get_safe_end_date('20260211')  # 假设现在是 19:00
        ('20260211', False)  # 保持今天
        
        >>> # 配置昨天
        >>> get_safe_end_date('20260210')  # 任何时间
        ('20260210', False)  # 保持昨天
    """
    now = datetime.now()
    current_time = now.time()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = (today - timedelta(days=1)).strftime('%Y%m%d')
    
    # 定义数据可用时间（18:30）
    # BaoStock 在 18:00 完成数据更新，留30分钟缓冲
    data_available_time = datetime.strptime('18:30', '%H:%M').time()
    
    # 解析用户配置的结束日期
    end_date_obj = datetime.strptime(end_date_str, '%Y%m%d')
    is_fetching_today = end_date_obj.date() == today.date()
    
    # 如果用户想获取今日数据，但当前时间早于18:30，强制使用昨天
    if is_fetching_today and current_time < data_available_time:
        print("=" * 60)
        print("⚠️  数据获取时间限制")
        print("=" * 60)
        print(f"当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"配置的结束日期: {end_date_str} (今天)")
        print()
        print("📋 说明:")
        print("  - BaoStock 数据在 18:00 后才完整更新")
        print("  - 为确保数据完整性，18:30 之前禁止获取当日数据")
        print()
        print(f"✅ 已自动调整结束日期为: {yesterday} (昨天)")
        print(f"💡 提示: 请在 18:30 之后运行以获取今日数据")
        print("=" * 60)
        print()
        return yesterday, True
    
    return end_date_str, False
