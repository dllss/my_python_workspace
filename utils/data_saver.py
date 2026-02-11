"""
数据保存工具

提供数据保存、合并和过滤的功能
"""

import os
import logging
import pandas as pd
from typing import Tuple, Optional
from .metadata_manager import MetadataManager

logger = logging.getLogger(__name__)


def filter_suspended_trading_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    过滤停牌交易数据
    
    停牌特征：成交量为空 或 成交额为空
    
    Args:
        df: 原始数据DataFrame
    
    Returns:
        Tuple[DataFrame, int]: (过滤后的DataFrame, 移除的记录数)
    """
    if df.empty:
        return df, 0
    
    # 兼容中英文列名
    volume_col = '成交量' if '成交量' in df.columns else 'volume'
    amount_col = '成交额' if '成交额' in df.columns else 'amount'
    
    if volume_col in df.columns and amount_col in df.columns:
        original_count = len(df)
        
        # 过滤：保留成交量和成交额都不为空且大于0的记录
        df_filtered = df[
            (df[volume_col].notna()) &
            (df[amount_col].notna()) &
            (df[volume_col] > 0) &
            (df[amount_col] > 0)
        ].copy()
        
        removed_count = original_count - len(df_filtered)
        
        # 返回过滤结果（不输出日志，由调用方决定是否显示）
        return df_filtered, removed_count
    
    return df, 0


def save_dataframe(df: pd.DataFrame, output_file: str, stock_code: str) -> None:
    """
    保存DataFrame到CSV文件，确保股票代码格式正确
    
    Args:
        df: 要保存的DataFrame
        output_file: 输出文件路径
        stock_code: 股票代码（用于确保前导零）
    """
    # 兼容中英文列名
    date_col = '日期' if '日期' in df.columns else 'date'
    df[date_col] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
    
    # 确保股票代码为字符串类型（保留前导零）
    if '股票代码' in df.columns:
        df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
    
    df.to_csv(output_file, index=False, encoding="utf-8-sig")


def merge_and_save_data(
    df_new: pd.DataFrame,
    output_file: str,
    stock_code: str,
    need_full_refresh: bool,
    metadata_manager: Optional[MetadataManager] = None,
    fetch_end_date: Optional[str] = None
) -> Tuple[bool, int, int]:
    """
    合并新旧数据并保存
    
    保留历史名称策略：
    - 不修改历史数据中的股票名称
    - 新增数据使用当前的最新名称
    - 这样可以记录股票名称的变化历史（如何时变为 ST）
    
    Args:
        df_new: 新获取的数据
        output_file: 输出文件路径
        stock_code: 股票代码
        need_full_refresh: 是否完全刷新
        metadata_manager: 元数据管理器（可选）
        fetch_end_date: 实际请求的结束日期（格式 YYYYMMDD），用于更新元数据（可选）
    
    Returns:
        Tuple[is_update, new_count, removed_count]: (是否为更新操作, 新增数据条数, 过滤的停牌数据条数)
    """
    # 兼容中英文列名
    date_col = '日期' if '日期' in df_new.columns else 'date'
    
    # 过滤新数据中的停牌记录
    df_new_filtered, removed_count_new = filter_suspended_trading_data(df_new)
    
    if df_new_filtered.empty:
        # 新数据全部为停牌数据，跳过保存（不输出日志）
        # 但仍需更新元数据，避免重复拉取
        if metadata_manager is not None and fetch_end_date:
            metadata_manager.update_last_date(stock_code, fetch_end_date)
        return False, 0, removed_count_new
    
    removed_count_total = removed_count_new
    
    if os.path.exists(output_file) and not need_full_refresh:
        # 增量更新：保留历史数据的原始名称，新数据使用最新名称
        df_existing = pd.read_csv(output_file, dtype={'股票代码': str})
        
        # 也过滤历史数据中的停牌记录
        df_existing_filtered, removed_count_existing = filter_suspended_trading_data(df_existing)
        removed_count_total += removed_count_existing
        
        df_combined = pd.concat([df_existing_filtered, df_new_filtered], ignore_index=True)
        
        df_combined[date_col] = pd.to_datetime(df_combined[date_col])
        df_combined = df_combined.drop_duplicates(subset=[date_col]).sort_values(by=date_col)
        
        save_dataframe(df_combined, output_file, stock_code)
        is_update = True
        new_count = len(df_new_filtered)
    else:
        # 新文件或完全刷新
        save_dataframe(df_new_filtered, output_file, stock_code)
        is_update = os.path.exists(output_file) and need_full_refresh
        new_count = len(df_new_filtered)
    
    # 更新元数据
    # 优先使用请求的结束日期，避免因停牌导致重复拉取
    if metadata_manager is not None:
        if fetch_end_date:
            # 使用实际请求的结束日期
            metadata_manager.update_last_date(stock_code, fetch_end_date)
        elif not df_new_filtered.empty:
            # 降级方案：使用实际数据的最大日期
            df_temp = df_new_filtered.copy()
            df_temp[date_col] = pd.to_datetime(df_temp[date_col])
            last_date = df_temp[date_col].max().strftime('%Y%m%d')
            metadata_manager.update_last_date(stock_code, last_date)
    
    return is_update, new_count, removed_count_total
