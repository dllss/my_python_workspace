# -*- coding: utf-8 -*-
"""
缺失日期范围检查工具
"""

import os
import pandas as pd
from datetime import timedelta
from typing import Tuple, Optional, List
from .trading_day_checker import has_trading_day
from .metadata_manager import MetadataManager


def get_missing_date_range(
    existing_file: str,
    start_date: str,
    end_date: str,
    update_mode: str = 'tail',
    metadata_manager: Optional[MetadataManager] = None
) -> Tuple[bool, Optional[str], Optional[str], bool, Optional[List[str]]]:
    """
    检查已存在的股票数据文件，分析缺失的日期范围
    
    Args:
        existing_file (str): 已存在的CSV文件路径
        start_date (str): 目标开始日期，格式 'YYYYMMDD'（如 '20000101'）
        end_date (str): 目标结束日期，格式 'YYYYMMDD'（如 '20000110'）
        update_mode (str): 更新模式，可选值：
            - 'tail': 只补充尾部数据（默认，推荐）
            - 'full': 完全刷新，补充所有缺失数据（包括中间停牌日）
            - 'head_tail': 补充头部和尾部，忽略中间缺失
    
    Returns:
        tuple: (need_update, fetch_start, fetch_end, need_full_refresh, missing_dates_list)
            - need_update (bool): 是否需要更新数据
            - fetch_start (str|None): 需要获取的开始日期，格式 'YYYYMMDD'；如果不需要更新则为 None
            - fetch_end (str|None): 需要获取的结束日期，格式 'YYYYMMDD'；如果不需要更新则为 None
            - need_full_refresh (bool): 是否需要完全刷新（中间有缺失日期时为 True）
            - missing_dates_list (list|None): 缺失的日期列表，格式 ['YYYY-MM-DD', ...]；如果不需要更新则为 None
    
    Examples:
        >>> # 文件不存在，需要获取全部数据
        >>> get_missing_date_range('stock_000001.csv', '20000101', '20000110')
        (True, '20000101', '20000110', False, None)
        
        >>> # 文件已有 20000104~20000110 的数据，缺失头部
        >>> get_missing_date_range('stock_000001.csv', '20000101', '20000110')
        (True, '20000101', '20000103', False, ['2000-01-01', '2000-01-02', '2000-01-03'])
        
        >>> # 文件已有 20000101~20000105 的数据，缺失尾部（默认模式）
        >>> get_missing_date_range('stock_000001.csv', '20000101', '20000110')
        (True, '20000106', '20000110', False, ['2000-01-06', '2000-01-07', ...])
        
        >>> # 文件已有全部数据，不需要更新
        >>> get_missing_date_range('stock_000001.csv', '20000101', '20000110')
        (False, None, None, False, None)
        
        >>> # 文件中间有缺失，update_mode='tail' 时忽略（可能是停牌）
        >>> get_missing_date_range('stock_000001.csv', '20000101', '20000110', update_mode='tail')
        (False, None, None, False, None)
        
        >>> # 文件中间有缺失，update_mode='full' 时完全刷新
        >>> get_missing_date_range('stock_000001.csv', '20000101', '20000110', update_mode='full')
        (True, '20000101', '20000110', True, ['2000-01-05', '2000-01-07'])
    """
    if not os.path.exists(existing_file):
        return (True, start_date, end_date, False, None)
    
    try:
        # 超快速路径：使用元数据（如果提供）
        if update_mode == 'tail' and metadata_manager is not None:
            # 从文件名提取股票代码
            stock_code = os.path.basename(existing_file).replace('stock_', '').replace('.csv', '')
            last_date = metadata_manager.get_last_date(stock_code)
            
            if last_date:
                # 元数据存在，直接使用
                if last_date >= end_date:
                    return (False, None, None, False, None)
                
                # 需要更新尾部数据
                next_day = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y%m%d')
                
                # 检查是否有交易日需要更新
                if has_trading_day(next_day, end_date):
                    return (True, next_day, end_date, False, None)
                else:
                    # 没有交易日需要更新
                    return (False, None, None, False, None)
        
        # 快速路径：tail 模式下只需要检查最后一行
        if update_mode == 'tail':
            # 只读取最后一行来判断是否需要更新
            try:
                last_line_df = pd.read_csv(existing_file, nrows=1)  # 先读取第一行获取列名
                if last_line_df.empty:
                    return (True, start_date, end_date, False, None)
                
                # 获取日期列名
                date_col = '日期' if '日期' in last_line_df.columns else 'date'
                
                # 只读取最后一行
                last_line_df = pd.read_csv(existing_file, usecols=[date_col])
                if last_line_df.empty:
                    return (True, start_date, end_date, False, None)
                
                # 获取最后一行的日期
                last_date_str = last_line_df[date_col].iloc[-1]
                existing_end = pd.to_datetime(last_date_str).strftime('%Y%m%d')
                
                # 如果最后一行日期 >= 目标结束日期，说明已是最新
                if existing_end >= end_date:
                    return (False, None, None, False, None)
                
                # 需要更新尾部数据
                next_day = (pd.to_datetime(existing_end) + timedelta(days=1)).strftime('%Y%m%d')
                
                # 检查是否有交易日需要更新
                if has_trading_day(next_day, end_date):
                    return (True, next_day, end_date, False, None)
                else:
                    # 没有交易日需要更新
                    return (False, None, None, False, None)
            except Exception:
                # 如果快速路径失败，降级到完整读取
                pass
        
        # 完整路径：读取整个文件（用于 full 和 head_tail 模式，或 tail 模式快速路径失败时）
        existing_df = pd.read_csv(existing_file)
        if existing_df.empty:
            return (True, start_date, end_date, False, None)
        
        # 获取已有数据的日期（兼容中英文列名）
        date_col = '日期' if '日期' in existing_df.columns else 'date'
        existing_df[date_col] = pd.to_datetime(existing_df[date_col])
        existing_dates = set(existing_df[date_col].dt.strftime('%Y%m%d'))
        
        existing_start = existing_df[date_col].min().strftime('%Y%m%d')
        existing_end = existing_df[date_col].max().strftime('%Y%m%d')
        
        # 生成目标日期范围
        target_start = pd.to_datetime(start_date)
        target_end = pd.to_datetime(end_date)
        all_dates = pd.date_range(start=target_start, end=target_end, freq='D')
        target_dates = set(all_dates.strftime('%Y%m%d'))
        
        # 检查缺失的日期
        missing_dates = target_dates - existing_dates
        
        if not missing_dates:
            return (False, None, None, False, None)
        
        # 过滤出缺失的交易日（排除周末和节假日）
        missing_trading_days = []
        for date_str in missing_dates:
            # 使用 has_trading_day 判断单个日期是否为交易日
            if has_trading_day(date_str, date_str):
                missing_trading_days.append(date_str)
        
        # 如果没有缺失的交易日，说明只是缺少周末/节假日，不需要更新
        if not missing_trading_days:
            return (False, None, None, False, None)
        
        # 有缺失的交易日
        missing_sorted = sorted(missing_trading_days)
        
        # 格式化缺失日期列表为 YYYY-MM-DD 格式
        missing_dates_formatted = [pd.to_datetime(d).strftime('%Y-%m-%d') for d in missing_sorted]
        
        # 判断缺失位置类型
        has_head_missing = missing_sorted[0] < existing_start  # 头部有缺失
        has_tail_missing = missing_sorted[-1] > existing_end   # 尾部有缺失
        
        # 判断中间缺失：只有当缺失日期严格在现有数据范围内部时，才算中间缺失
        # 注意：如果缺失的日期都在头部或尾部，则不算中间缺失
        middle_missing_dates = [d for d in missing_sorted if existing_start < d < existing_end]
        has_middle_missing = len(middle_missing_dates) > 0
        
        # 根据更新模式处理中间缺失
        if has_middle_missing:
            if update_mode == 'full':
                # 完全刷新模式：补充所有缺失（包括中间可能的停牌日）
                return (True, start_date, end_date, True, missing_dates_formatted)
            elif update_mode == 'tail':
                # 尾部模式（默认）：忽略中间缺失（可能是停牌），只处理尾部
                if has_tail_missing:
                    next_day = (pd.to_datetime(existing_end) + timedelta(days=1)).strftime('%Y%m%d')
                    return (True, next_day, end_date, False, missing_dates_formatted)
                else:
                    # 只有中间缺失，不更新
                    return (False, None, None, False, None)
            elif update_mode == 'head_tail':
                # 头尾模式：补充头部和尾部，忽略中间
                if has_head_missing and has_tail_missing:
                    # 同时有头尾缺失，需要分两次获取（这里简化为完全刷新）
                    return (True, start_date, end_date, True, missing_dates_formatted)
                elif has_tail_missing:
                    next_day = (pd.to_datetime(existing_end) + timedelta(days=1)).strftime('%Y%m%d')
                    return (True, next_day, end_date, False, missing_dates_formatted)
                elif has_head_missing:
                    prev_day = (pd.to_datetime(existing_start) - timedelta(days=1)).strftime('%Y%m%d')
                    return (True, start_date, prev_day, False, missing_dates_formatted)
                else:
                    # 只有中间缺失，不更新
                    return (False, None, None, False, None)
        
        # 没有中间缺失的情况，根据 update_mode 处理
        if update_mode == 'tail':
            # tail 模式：只处理尾部，忽略头部
            if has_tail_missing:
                next_day = (pd.to_datetime(existing_end) + timedelta(days=1)).strftime('%Y%m%d')
                return (True, next_day, end_date, False, missing_dates_formatted)
            else:
                # 只有头部缺失或没有缺失，不更新
                return (False, None, None, False, None)
        
        # 其他模式：处理头部和尾部缺失
        # 只有尾部缺失
        if has_tail_missing and not has_head_missing:
            next_day = (pd.to_datetime(existing_end) + timedelta(days=1)).strftime('%Y%m%d')
            return (True, next_day, end_date, False, missing_dates_formatted)
        
        # 只有头部缺失
        if has_head_missing and not has_tail_missing:
            prev_day = (pd.to_datetime(existing_start) - timedelta(days=1)).strftime('%Y%m%d')
            return (True, start_date, prev_day, False, missing_dates_formatted)
        
        # 同时有头部和尾部缺失
        if has_head_missing and has_tail_missing:
            return (True, start_date, end_date, True, missing_dates_formatted)
        
        # 理论上不会到这里
        return (False, None, None, False, None)
        
    except Exception as e:
        return (True, start_date, end_date, False, None)
