#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量获取所有A股当日行情数据（高效版）
使用 AkShare 的批量接口一次性获取所有股票数据，而非逐个轮询

使用方法：
    方式1（推荐）：
        make run-daily
    
    方式2：
        poetry run python fetch_daily_all_stocks.py
    
    方式3：
        python fetch_daily_all_stocks.py  # 需要先激活虚拟环境

主要优势：
    ✅ 一次性获取所有股票数据（无需逐个轮询）
    ✅ 速度快（几秒内完成，而非几小时）
    ✅ 无需延迟（批量接口不会触发限流）
    ✅ 自动更新元数据
    ✅ 适合每日定时任务

注意事项：
    ⚠️ 建议在 18:30 之后运行（确保数据完整）
    ⚠️ 首次运行前需先获取股票列表：make run-list
    ⚠️ 首次运行前需先获取历史数据：make run-all-v2
"""

import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import akshare as ak
from config import OUTPUT_DIR, CN_DIR
from utils import MetadataManager, save_dataframe, get_safe_end_date, filter_suspended_trading_data

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# ========== 常量定义 ==========
MAX_ERROR_MESSAGE_LENGTH = 50

def fetch_all_stocks_daily_data(target_date: str = None) -> pd.DataFrame:
    """
    使用 AkShare 批量接口获取所有A股当日行情数据
    
    Args:
        target_date: 目标日期（格式 YYYYMMDD），默认为今天
    
    Returns:
        DataFrame: 所有股票的当日数据
    """
    try:
        logger.info("正在获取所有A股实时行情数据...")
        
        # 使用 AkShare 批量接口
        df = ak.stock_zh_a_spot_em()
        
        if df.empty:
            logger.warning("未获取到任何数据")
            return None
        
        logger.info(f"成功获取 {len(df)} 只股票的数据")
        
        # 标准化列名（AkShare 的列名可能不同）
        # 需要根据实际返回的列名进行映射
        column_mapping = {
            '代码': '股票代码',
            '名称': '股票名称',
            '最新价': '收盘',
            '涨跌幅': '涨跌幅',
            '涨跌额': '涨跌额',
            '成交量': '成交量',
            '成交额': '成交额',
            '振幅': '振幅',
            '最高': '最高',
            '最低': '最低',
            '今开': '开盘',
            '昨收': '昨收',
            '换手率': '换手率'
        }
        
        # 重命名列
        df = df.rename(columns=column_mapping)
        
        # 添加日期列
        if target_date:
            date_str = pd.to_datetime(target_date).strftime('%Y-%m-%d')
        else:
            date_str = datetime.now().strftime('%Y-%m-%d')
        df['日期'] = date_str
        
        # 确保股票代码为6位字符串
        df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
        
        # 调整列顺序
        column_order = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        df = df[[col for col in column_order if col in df.columns]]
        
        return df
        
    except Exception as e:
        logger.error(f"获取数据失败: {str(e)[:MAX_ERROR_MESSAGE_LENGTH]}")
        return None


def update_all_stock_files(df_all: pd.DataFrame, cn_dir: str, metadata_mgr: MetadataManager) -> dict:
    """
    将批量获取的数据更新到各个股票的CSV文件
    
    Args:
        df_all: 所有股票的当日数据
        cn_dir: 数据目录
        metadata_mgr: 元数据管理器
    
    Returns:
        dict: 统计信息
    """
    success_count = 0
    update_count = 0
    skip_count = 0
    fail_count = 0
    
    total = len(df_all)
    date_str = df_all['日期'].iloc[0]
    target_date = pd.to_datetime(date_str).strftime('%Y%m%d')
    
    logger.info(f"开始更新 {total} 只股票的数据到文件...")
    logger.info(f"目标日期: {date_str}")
    print()
    
    for index, row in df_all.iterrows():
        stock_code = row['股票代码']
        stock_name = row.get('股票名称', stock_code)
        
        # 显示进度
        if (index + 1) % 100 == 0 or (index + 1) == total:
            print(f"\r处理进度: [{index + 1}/{total}] {stock_code} {stock_name}...", end="", flush=True)
        
        try:
            output_file = os.path.join(cn_dir, f"stock_{stock_code}.csv")
            
            # 检查是否需要更新
            last_date = metadata_mgr.get_last_date(stock_code)
            if last_date and last_date >= target_date:
                skip_count += 1
                continue
            
            # 准备当日数据
            df_new = pd.DataFrame([row])
            
            # 过滤停牌数据
            df_new_filtered, removed_count = filter_suspended_trading_data(df_new)
            
            # 如果是停牌数据，跳过保存但仍更新元数据
            if df_new_filtered.empty:
                skip_count += 1
                # 更新元数据，避免下次重复检查
                metadata_mgr.update_last_date(stock_code, target_date)
                continue
            
            # 合并并保存（保留历史名称策略：不修改历史数据，新数据使用最新名称）
            if os.path.exists(output_file):
                # 增量更新
                df_existing = pd.read_csv(output_file, dtype={'股票代码': str})
                df_combined = pd.concat([df_existing, df_new_filtered], ignore_index=True)
                df_combined['日期'] = pd.to_datetime(df_combined['日期'])
                df_combined = df_combined.drop_duplicates(subset=['日期']).sort_values(by='日期')
                save_dataframe(df_combined, output_file, stock_code)
                update_count += 1
            else:
                # 新文件
                save_dataframe(df_new_filtered, output_file, stock_code)
                success_count += 1
            
            # 更新元数据
            metadata_mgr.update_last_date(stock_code, target_date)
            
        except Exception as e:
            fail_count += 1
            logger.debug(f"更新 {stock_code} 失败: {str(e)[:MAX_ERROR_MESSAGE_LENGTH]}")
            continue
    
    print()  # 换行
    print()
    
    return {
        'success': success_count,
        'update': update_count,
        'skip': skip_count,
        'fail': fail_count,
        'total': total
    }


def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("批量获取所有A股当日行情数据")
    logger.info("=" * 60)
    print()
    
    # 初始化
    cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
    
    # 检查并调整结束日期（18:30之前强制使用昨天）
    today = datetime.now().strftime('%Y%m%d')
    safe_date, date_adjusted = get_safe_end_date(today)
    
    if date_adjusted:
        logger.info(f"注意: 已自动调整为获取 {safe_date} 的数据")
        print()
    
    # 初始化元数据管理器
    metadata_mgr = MetadataManager(cn_dir)
    
    # 获取所有股票数据
    df_all = fetch_all_stocks_daily_data(safe_date)
    
    if df_all is None or df_all.empty:
        logger.error("获取数据失败，退出")
        return
    
    print()
    
    # 更新到各个文件
    stats = update_all_stock_files(df_all, cn_dir, metadata_mgr)
    
    # 显示统计信息
    logger.info("=" * 60)
    logger.info("统计信息")
    logger.info("=" * 60)
    logger.info(f"新增: {stats['success']} 只")
    logger.info(f"更新: {stats['update']} 只")
    logger.info(f"跳过: {stats['skip']} 只（已是最新）")
    logger.info(f"失败: {stats['fail']} 只")
    logger.info(f"总计: {stats['total']} 只")
    logger.info("=" * 60)
    
    # 提示
    if stats['fail'] > 0:
        logger.warning(f"⚠️  有 {stats['fail']} 只股票更新失败")
    else:
        logger.info("✅ 所有股票数据更新完成")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  用户中断")
    except Exception as e:
        logger.error(f"\n\n❌ 发生错误: {str(e)}")
        raise
