#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
判断指定日期是否为A股交易日

使用方法：
    方式1（推荐）：
        make check-date DATE=20000103
    
    方式2：
        poetry run python check_trading_day.py 20000103
    
    方式3：
        python check_trading_day.py 2000-01-03  # 需要先激活虚拟环境

支持的日期格式：
    - YYYYMMDD：20000103
    - YYYY-MM-DD：2000-01-03
    - YYYY/MM/DD：2000/01/03

功能说明：
    - 使用 exchange_calendars 专业交易日历
    - 准确识别A股交易日（包括节假日、调休工作日等特殊情况）
    - 显示查询日期前后各3天的情况
    - 支持数据范围：2000-01-04 至 2026-12-31

输出示例：
    查询结果：
    ============================================================
    日期: 2000-01-03（星期一）
    状态: ✅ 是交易日
    ============================================================
    
    前后几天的情况：
    ------------------------------------------------------------
    2000-01-01（星期六）  ❌ 休市
    2000-01-02（星期日）  ❌ 休市
    2000-01-03（星期一）  ✅ 交易日  ← 查询日期
    2000-01-04（星期二）  ✅ 交易日
    ...

注意事项：
    - 2000年之前的数据会降级为周末判断
    - 调休工作日不一定是股市交易日（如春节后周日上班但股市休市）
"""

import sys
import re
from utils import has_trading_day
import pandas as pd


def parse_date(date_input):
    """
    解析日期输入，支持多种格式
    
    Args:
        date_input: 日期字符串，支持格式：
                   - YYYYMMDD (20000103)
                   - YYYY-MM-DD (2000-01-03)
                   - YYYY/MM/DD (2000/01/03)
    
    Returns:
        str: 标准格式 YYYYMMDD
    """
    # 移除所有分隔符
    clean_date = re.sub(r'[-/]', '', date_input)
    
    # 验证格式
    if not re.match(r'^\d{8}$', clean_date):
        raise ValueError(f"无效的日期格式: {date_input}")
    
    # 验证日期有效性
    try:
        pd.to_datetime(clean_date)
    except Exception:
        raise ValueError(f"无效的日期: {date_input}")
    
    return clean_date


def check_trading_day(date_str):
    """
    检查指定日期是否为交易日
    
    Args:
        date_str: 日期字符串 (YYYYMMDD)
    
    Returns:
        bool: 是否为交易日
    """
    return has_trading_day(date_str, date_str)


def format_date_info(date_str):
    """
    格式化日期信息
    
    Args:
        date_str: 日期字符串 (YYYYMMDD)
    
    Returns:
        str: 格式化的日期信息
    """
    date_obj = pd.to_datetime(date_str)
    weekday_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    weekday = weekday_names[date_obj.weekday()]
    formatted = date_obj.strftime('%Y-%m-%d')
    
    return f"{formatted} ({weekday})"


def show_usage():
    """显示使用说明"""
    print("=" * 60)
    print("A股交易日查询工具")
    print("=" * 60)
    print()
    print("使用方法：")
    print("  python check_trading_day.py <日期>")
    print()
    print("日期格式：")
    print("  - YYYYMMDD    例如: 20000103")
    print("  - YYYY-MM-DD  例如: 2000-01-03")
    print("  - YYYY/MM/DD  例如: 2000/01/03")
    print()
    print("示例：")
    print("  python check_trading_day.py 20000103")
    print("  python check_trading_day.py 2024-02-18")
    print()


def main():
    """主函数"""
    # 检查参数
    if len(sys.argv) < 2:
        show_usage()
        sys.exit(1)
    
    date_input = sys.argv[1]
    
    # 显示帮助
    if date_input in ['-h', '--help', 'help']:
        show_usage()
        sys.exit(0)
    
    try:
        # 解析日期
        date_str = parse_date(date_input)
        date_info = format_date_info(date_str)
        
        # 检查是否为交易日
        is_trading = check_trading_day(date_str)
        
        # 输出结果
        print()
        print("查询结果：")
        print("=" * 60)
        print(f"日期: {date_info}")
        print(f"状态: {'✅ 是交易日' if is_trading else '❌ 不是交易日（休市）'}")
        print("=" * 60)
        print()
        
        # 显示前后几天的情况
        print("前后几天的情况：")
        print("-" * 60)
        
        date_obj = pd.to_datetime(date_str)
        date_range = pd.date_range(
            start=date_obj - pd.Timedelta(days=3),
            end=date_obj + pd.Timedelta(days=3),
            freq='D'
        )
        
        for d in date_range:
            d_str = d.strftime('%Y%m%d')
            d_info = format_date_info(d_str)
            d_is_trading = check_trading_day(d_str)
            status = "✅ 交易日" if d_is_trading else "❌ 休市"
            
            highlight = "  ← 查询日期" if d.date() == date_obj.date() else ""
            print(f"{d_info}  {status}{highlight}")
        
        print()
        
    except ValueError as e:
        print(f"错误: {e}")
        print()
        show_usage()
        sys.exit(1)
    except Exception as e:
        print(f"发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
