#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量获取所有A股当日行情数据（TuShare版）
使用 TuShare 的批量接口一次性获取所有股票数据

使用方法：
    方式1（推荐）：
        make daily-tushare
    
    方式2：
        poetry run python fetch_daily_data_tushare.py
    
    方式3：
        python fetch_daily_data_tushare.py  # 需要先激活虚拟环境

主要优势：
    ✅ 一次性获取所有股票数据（无需逐个轮询）
    ✅ 速度快（几秒内完成，而非几小时）
    ✅ 无需延迟（批量接口不会触发限流）
    ✅ 自动更新元数据
    ✅ 网络稳定（无需访问东方财富）
    ✅ 数据质量高

配置要求：
    ⚠️ 需要 TuShare Token（注册即可免费获得）
    ⚠️ 需要至少 120 积分（注册填写信息即可获得）
    ⚠️ 建议在 17:00 之后运行（TuShare 数据在 15:00-17:00 更新）

Token 配置方式：
    方式1（推荐）：设置环境变量
        export TUSHARE_TOKEN="你的token"
    
    方式2：在 config.py 中配置
        TUSHARE_TOKEN = "你的token"
    
    方式3：在命令行参数中指定
        python fetch_daily_data_tushare.py --token "你的token"

获取 Token：
    1. 注册账号: https://tushare.pro/register
    2. 获取 token: https://tushare.pro/user/token
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Optional, Tuple
import pandas as pd

try:
    import tushare as ts
except ImportError:
    print("错误: 未安装 tushare 库")
    print("请运行: poetry add tushare")
    print("或者: pip install tushare")
    sys.exit(1)

from config import OUTPUT_DIR, CN_DIR
from utils import MetadataManager, save_dataframe, get_safe_end_date, filter_suspended_trading_data

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


def get_tushare_token() -> Optional[str]:
    """
    获取 TuShare Token
    优先级: 命令行参数 > config.py > 环境变量
    
    Returns:
        str: TuShare Token，如果未配置则返回 None
    """
    # 1. 尝试从 config.py 获取（优先）
    try:
        from config import TUSHARE_TOKEN
        if TUSHARE_TOKEN:
            return TUSHARE_TOKEN
    except ImportError:
        pass
    
    # 2. 尝试从环境变量获取
    token = os.environ.get('TUSHARE_TOKEN')
    if token:
        return token
    
    return None


def fetch_all_stocks_daily_data_tushare(
    token: str,
    target_date: str = None
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    使用 TuShare 批量接口获取所有A股当日行情数据
    
    Args:
        token: TuShare API Token
        target_date: 目标日期（格式 YYYYMMDD），默认为昨天
    
    Returns:
        Tuple[DataFrame, str]: (所有股票的当日数据, 实际查询日期)
    """
    try:
        # 初始化 TuShare Pro API
        pro = ts.pro_api(token)
        
        # 确定目标日期（默认为昨天，因为今天的数据可能还未更新）
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        
        logger.info("="*80)
        logger.info("TuShare 批量获取A股日线数据")
        logger.info("="*80)
        logger.info(f"目标日期: {target_date}")
        logger.info(f"数据源: TuShare Pro (官方接口)")
        logger.info(f"接口: daily (日线行情)")
        logger.info("")
        logger.info("正在获取数据...")
        
        # 使用 TuShare 批量接口获取指定日期的所有股票数据
        df = pro.daily(trade_date=target_date)
        
        if df is None or df.empty:
            logger.warning(f"未获取到 {target_date} 的数据")
            logger.warning("可能原因:")
            logger.warning("  1. 该日期为非交易日（周末/节假日）")
            logger.warning("  2. 数据尚未更新（建议在17:00后运行）")
            logger.warning("  3. Token 权限不足")
            return None, target_date
        
        logger.info(f"✅ 成功获取 {len(df)} 只股票的数据")
        logger.info("")
        
        return df, target_date
        
    except Exception as e:
        logger.error(f"获取数据失败: {e}")
        logger.error("")
        logger.error("常见错误解决方案:")
        logger.error("  1. Token 无效: 请检查 token 是否正确")
        logger.error("  2. 积分不足: 需要至少 120 积分（注册填写信息即可获得）")
        logger.error("  3. 调用频率超限: 基础用户每分钟最多 50 次")
        logger.error("  4. 网络连接失败: 检查网络连接")
        logger.error("")
        logger.error("获取 Token: https://tushare.pro/user/token")
        return None, target_date


def convert_tushare_to_standard_format(df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """
    将 TuShare 数据格式转换为项目标准格式
    
    TuShare 列名:
        ts_code, trade_date, open, high, low, close, pre_close, 
        change, pct_chg, vol, amount
    
    标准格式列名:
        日期, 股票代码, 股票名称, 开盘, 最高, 最低, 收盘, 
        成交量, 成交额, 涨跌幅, 涨跌额, 换手率
    
    Args:
        df: TuShare 原始数据
        target_date: 目标日期
    
    Returns:
        DataFrame: 标准格式的数据
    """
    # 1. 提取股票代码（去掉市场后缀）
    df['股票代码'] = df['ts_code'].str.split('.').str[0]
    
    # 2. 添加日期列（TuShare 的 trade_date 格式为 YYYYMMDD）
    df['日期'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
    
    # 3. 列名映射
    column_mapping = {
        'open': '开盘',
        'high': '最高',
        'low': '最低',
        'close': '收盘',
        'vol': '成交量',      # TuShare 单位：手（100股）
        'amount': '成交额',   # TuShare 单位：千元
        'pct_chg': '涨跌幅',
        'change': '涨跌额',
    }
    
    df = df.rename(columns=column_mapping)
    
    # 4. 单位转换
    # TuShare 成交量单位是手（100股），需要转换为股
    df['成交量'] = df['成交量'] * 100
    
    # TuShare 成交额单位是千元，需要转换为元
    df['成交额'] = df['成交额'] * 1000
    
    # 5. 添加股票名称（TuShare daily 接口不返回名称，需要从股票列表获取）
    # 这里先设置为空，后续可以通过 stock_basic 接口补充
    df['股票名称'] = ''
    
    # 6. 添加换手率（TuShare daily 接口不返回换手率）
    df['换手率'] = 0.0
    
    # 7. 选择并排序列
    standard_columns = [
        '日期', '股票代码', '股票名称', '开盘', '最高', '最低', '收盘',
        '成交量', '成交额', '涨跌幅', '涨跌额', '换手率'
    ]
    
    df = df[standard_columns]
    
    # 8. 数据类型转换
    df['开盘'] = df['开盘'].astype(float).round(2)
    df['最高'] = df['最高'].astype(float).round(2)
    df['最低'] = df['最低'].astype(float).round(2)
    df['收盘'] = df['收盘'].astype(float).round(2)
    df['成交量'] = df['成交量'].astype(float).round(0)
    df['成交额'] = df['成交额'].astype(float).round(2)
    df['涨跌幅'] = df['涨跌幅'].astype(float).round(2)
    df['涨跌额'] = df['涨跌额'].astype(float).round(2)
    df['换手率'] = df['换手率'].astype(float).round(2)
    
    return df


def supplement_stock_names(df: pd.DataFrame, token: str) -> pd.DataFrame:
    """
    补充股票名称
    优先从本地 stock_list.csv 读取，失败则从 TuShare 获取
    
    Args:
        df: 标准格式的数据（缺少股票名称）
        token: TuShare API Token
    
    Returns:
        DataFrame: 补充了股票名称的数据
    """
    import time
    
    try:
        logger.info("正在补充股票名称...")
        
        # 方法1: 优先从本地文件读取（推荐，无频率限制）
        local_stock_list = os.path.join(OUTPUT_DIR, CN_DIR, 'stock_list.csv')
        if os.path.exists(local_stock_list):
            try:
                logger.info("从本地文件读取股票名称...")
                stock_list = pd.read_csv(local_stock_list, dtype={'code': str})
                stock_list.columns = ['股票代码', '股票名称']
                
                # 创建代码到名称的映射
                code_to_name = dict(zip(stock_list['股票代码'], stock_list['股票名称']))
                
                # 补充名称
                df['股票名称'] = df['股票代码'].map(code_to_name).fillna('')
                
                matched_count = df['股票名称'].ne('').sum()
                logger.info(f"✅ 成功从本地文件补充 {matched_count} 只股票的名称")
                
                return df
            except Exception as e:
                logger.warning(f"从本地文件读取失败: {e}，尝试从 TuShare 获取...")
        else:
            logger.warning(f"本地文件不存在: {local_stock_list}，尝试从 TuShare 获取...")
        
        # 方法2: 从 TuShare 获取（有频率限制）
        logger.info("从 TuShare 获取股票名称...")
        pro = ts.pro_api(token)
        max_retries = 3
        retry_delay = 60  # 秒
        
        for attempt in range(max_retries):
            try:
                stock_basic = pro.stock_basic(
                    exchange='',
                    list_status='L',
                    fields='ts_code,symbol,name'
                )
                break
            except Exception as e:
                error_msg = str(e)
                if "每分钟" in error_msg or "频率" in error_msg or "每小时" in error_msg:
                    if attempt < max_retries - 1:
                        logger.warning(f"调用频率限制，等待 {retry_delay} 秒后重试...")
                        time.sleep(retry_delay)
                    else:
                        logger.warning(f"补充股票名称失败（频率限制），股票名称将为空")
                        logger.warning(f"建议: 运行 'make list' 更新本地股票列表，下次可直接从本地读取")
                        return df
                else:
                    raise
        
        if stock_basic is None or stock_basic.empty:
            logger.warning("未能获取股票基本信息，股票名称将为空")
            return df
        
        # 提取股票代码（去掉市场后缀）
        stock_basic['股票代码'] = stock_basic['symbol']
        stock_basic['股票名称'] = stock_basic['name']
        
        # 创建代码到名称的映射
        code_to_name = dict(zip(stock_basic['股票代码'], stock_basic['股票名称']))
        
        # 补充名称
        df['股票名称'] = df['股票代码'].map(code_to_name).fillna('')
        
        logger.info(f"✅ 成功从 TuShare 补充 {df['股票名称'].ne('').sum()} 只股票的名称")
        
        return df
        
    except Exception as e:
        logger.warning(f"补充股票名称失败: {e}")
        logger.warning("股票名称将为空")
        return df


def process_and_save_daily_data(
    df: pd.DataFrame,
    target_date: str,
    metadata_mgr: MetadataManager
) -> Tuple[int, int, int]:
    """
    处理并保存每日数据到各个股票文件
    
    Args:
        df: 标准格式的数据
        target_date: 目标日期
        metadata_mgr: 元数据管理器
    
    Returns:
        Tuple[int, int, int]: (成功数, 跳过数, 失败数)
    """
    success_count = 0
    skip_count = 0
    failed_count = 0
    failed_stocks = []
    
    total = len(df)
    
    logger.info("="*80)
    logger.info("开始处理并保存数据")
    logger.info("="*80)
    
    # 按股票代码分组
    for stock_code, group_df in df.groupby('股票代码'):
        try:
            # 获取股票名称
            stock_name = group_df['股票名称'].iloc[0] if not group_df['股票名称'].empty else ''
            
            # 检查是否需要更新
            last_date = metadata_mgr.get_last_date(stock_code)
            if last_date and last_date >= target_date:
                skip_count += 1
                continue
            
            # 过滤停牌数据
            df_filtered, removed_count = filter_suspended_trading_data(group_df)
            
            # 如果过滤后为空，仍然更新元数据（避免重复拉取）
            if df_filtered.empty:
                metadata_mgr.update_last_date(stock_code, target_date)
                skip_count += 1
                continue
            
            # 保存数据
            file_path = os.path.join(OUTPUT_DIR, CN_DIR, f"{stock_code}.csv")
            
            # 如果文件存在，合并数据
            if os.path.exists(file_path):
                try:
                    df_existing = pd.read_csv(file_path, dtype={'股票代码': str})
                    df_merged = pd.concat([df_existing, df_filtered], ignore_index=True)
                    df_merged = df_merged.drop_duplicates(subset=['日期'], keep='last')
                    df_merged = df_merged.sort_values('日期')
                    save_dataframe(df_merged, file_path)
                except Exception as e:
                    logger.warning(f"合并数据失败 {stock_code}: {e}，将覆盖保存")
                    save_dataframe(df_filtered, file_path)
            else:
                save_dataframe(df_filtered, file_path)
            
            # 更新元数据
            metadata_mgr.update_last_date(stock_code, target_date)
            
            success_count += 1
            
            # 进度显示
            if success_count % 100 == 0:
                logger.info(f"进度: {success_count + skip_count}/{total} "
                          f"(成功: {success_count}, 跳过: {skip_count})")
            
        except Exception as e:
            failed_count += 1
            failed_stocks.append((stock_code, stock_name, str(e)))
            logger.error(f"处理失败 {stock_code} {stock_name}: {e}")
    
    return success_count, skip_count, failed_count, failed_stocks


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='使用 TuShare 批量获取A股日线数据')
    parser.add_argument('--token', type=str, help='TuShare API Token')
    parser.add_argument('--date', type=str, help='目标日期（格式 YYYYMMDD），默认为昨天')
    args = parser.parse_args()
    
    # 获取 Token
    token = args.token or get_tushare_token()
    
    if not token:
        logger.error("="*80)
        logger.error("错误: 未配置 TuShare Token")
        logger.error("="*80)
        logger.error("")
        logger.error("请通过以下方式之一配置 Token:")
        logger.error("")
        logger.error("方式1（推荐）：设置环境变量")
        logger.error('  export TUSHARE_TOKEN="你的token"')
        logger.error("")
        logger.error("方式2：在 config.py 中添加")
        logger.error('  TUSHARE_TOKEN = "你的token"')
        logger.error("")
        logger.error("方式3：命令行参数")
        logger.error('  python fetch_daily_data_tushare.py --token "你的token"')
        logger.error("")
        logger.error("获取 Token:")
        logger.error("  1. 注册账号: https://tushare.pro/register")
        logger.error("  2. 获取 token: https://tushare.pro/user/token")
        logger.error("="*80)
        sys.exit(1)
    
    # 确保输出目录存在
    os.makedirs(os.path.join(OUTPUT_DIR, CN_DIR), exist_ok=True)
    
    # 初始化元数据管理器
    metadata_mgr = MetadataManager(os.path.join(OUTPUT_DIR, CN_DIR))
    
    # 获取数据
    df_raw, target_date = fetch_all_stocks_daily_data_tushare(token, args.date)
    
    if df_raw is None:
        logger.error("获取数据失败，退出")
        sys.exit(1)
    
    # 保存原始数据到项目目录（CSV格式）
    raw_file = f'tushare_raw_data_{target_date}.csv'
    df_raw.to_csv(raw_file, index=False, encoding='utf-8-sig')
    logger.info(f"原始数据已保存到: {raw_file}")
    logger.info(f"  - 数据形状: {df_raw.shape}")
    logger.info(f"  - 总股票数: {len(df_raw)}")
    logger.info("")
    
    # 转换为标准格式
    logger.info("正在转换数据格式...")
    df_standard = convert_tushare_to_standard_format(df_raw, target_date)
    
    # 补充股票名称（在保存之前）
    df_standard = supplement_stock_names(df_standard, token)
    
    # 保存标准格式数据到项目目录（CSV格式）
    standard_file = f'tushare_standard_data_{target_date}.csv'
    df_standard.to_csv(standard_file, index=False, encoding='utf-8-sig')
    logger.info(f"标准格式数据已保存到: {standard_file}")
    logger.info(f"  - 数据形状: {df_standard.shape}")
    logger.info(f"  - 总股票数: {len(df_standard)}")
    logger.info("")
    
    # 处理并保存数据（暂时注释，不修改 data 文件夹）
    # success_count, skip_count, failed_count, failed_stocks = process_and_save_daily_data(
    #     df_standard,
    #     target_date,
    #     metadata_mgr
    # )
    
    # 输出统计信息
    logger.info("")
    logger.info("="*80)
    logger.info("执行完成")
    logger.info("="*80)
    logger.info(f"目标日期: {target_date}")
    logger.info(f"总股票数: {len(df_standard)}")
    logger.info(f"数据已保存到文件:")
    logger.info(f"  - 原始数据: {raw_file}")
    logger.info(f"  - 标准格式: {standard_file}")
    logger.info("")
    logger.info("⚠️  注意: 数据保存功能已暂时禁用（不修改 data 文件夹）")
    logger.info("   如需保存到 data 文件夹，请取消注释 process_and_save_daily_data 调用")
    # logger.info(f"成功保存: {success_count} 只")
    # logger.info(f"跳过更新: {skip_count} 只（已是最新）")
    # logger.info(f"失败数量: {failed_count} 只")
    # 
    # if failed_stocks:
    #     logger.info("")
    #     logger.info("失败列表:")
    #     for stock_code, stock_name, reason in failed_stocks:
    #         logger.info(f"  {stock_code} {stock_name}: {reason}")
    
    logger.info("="*80)


if __name__ == "__main__":
    main()
