#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
批量获取A股日线数据并保存为单个CSV文件（TuShare版）
使用 TuShare 的批量接口获取数据，所有股票保存在一个CSV文件中（而非分散到 data/ 目录）

使用方法：
    方式1（推荐）：获取所有股票
        poetry run python fetch_daily_data_tushare_to_single_file.py --date 20260210
    
    方式2：获取单只股票
        poetry run python fetch_daily_data_tushare_to_single_file.py --date 20260210 --code 000001
    
    方式3：获取多只股票
        poetry run python fetch_daily_data_tushare_to_single_file.py --date 20260210 --codes 000001,600519,000002
    
    方式4：指定输出文件名
        poetry run python fetch_daily_data_tushare_to_single_file.py --date 20260210 --output my_data.csv
    
    方式5：组合使用
        poetry run python fetch_daily_data_tushare_to_single_file.py --date 20260210 --codes 000001,600519 --output selected_stocks.csv

主要优势：
    ✅ 一次性获取所有股票数据（无需逐个轮询）
    ✅ 速度快（几秒内完成，而非几小时）
    ✅ 所有数据保存在一个CSV文件中（方便分析）
    ✅ 不修改原有的 data 文件夹
    ✅ 支持指定股票代码
    ✅ 支持自定义输出文件名

配置要求：
    ⚠️ 需要 TuShare Token（注册即可免费获得）
    ⚠️ 需要至少 120 积分（注册填写信息即可获得）
    ⚠️ 建议在 17:00 之后运行（TuShare 数据在 15:00-17:00 更新）

Token 配置方式：
    方式1（推荐）：在 config.py 中配置
        TUSHARE_TOKEN = "你的token"
    
    方式2：设置环境变量
        export TUSHARE_TOKEN="你的token"
    
    方式3：在命令行参数中指定
        python fetch_daily_data_tushare_to_single_file.py --token "你的token"

获取 Token：
    1. 注册账号: https://tushare.pro/register
    2. 获取 token: https://tushare.pro/user/token

参数说明：
    --date       目标日期，格式 YYYYMMDD（必填参数）
    --token      TuShare API Token（可选，优先从 config.py 读取）
    --code       单个股票代码（可选），例如: 000001
    --codes      多个股票代码（可选），逗号分隔，例如: 000001,600519,000002
    --output     输出文件名（可选），默认为: all_stocks_YYYYMMDD.csv
    --no-filter  不过滤停牌数据（可选），默认会过滤停牌数据
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

from config import OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE
from utils import filter_suspended_trading_data

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
    target_date: str
) -> Tuple[Optional[pd.DataFrame], str]:
    """
    使用 TuShare 批量接口获取所有A股当日行情数据
    
    Args:
        token: TuShare API Token
        target_date: 目标日期（格式 YYYYMMDD），必填参数
    
    Returns:
        Tuple[DataFrame, str]: (所有股票的当日数据, 实际查询日期)
    """
    try:
        # 初始化 TuShare Pro API
        pro = ts.pro_api(token)
        
        logger.info("="*80)
        logger.info("TuShare 批量获取A股日线数据（保存为单个CSV文件）")
        logger.info("="*80)
        logger.info(f"目标日期: {target_date}")
        logger.info(f"数据源: TuShare Pro (官方接口)")
        logger.info(f"接口: daily (日线行情)")
        logger.info("")
        logger.info("正在获取日线数据...")
        
        # 使用 TuShare 批量接口获取指定日期的所有股票数据
        df = pro.daily(trade_date=target_date)
        
        if df is None or df.empty:
            logger.warning(f"未获取到 {target_date} 的数据")
            logger.warning("可能原因:")
            logger.warning("  1. 该日期为非交易日（周末/节假日）")
            logger.warning("  2. 数据尚未更新（建议在17:00后运行）")
            logger.warning("  3. Token 权限不足")
            return None, target_date
        
        logger.info(f"✅ 成功获取 {len(df)} 只股票的日线数据")
        logger.info("")
        
        # 获取换手率等每日指标数据
        logger.info("正在获取每日指标数据（换手率等）...")
        try:
            df_basic = pro.daily_basic(trade_date=target_date, fields='ts_code,turnover_rate')
            if df_basic is not None and not df_basic.empty:
                logger.info(f"✅ 成功获取 {len(df_basic)} 只股票的每日指标")
                
                # 检查换手率数据
                non_zero_count = (df_basic['turnover_rate'] > 0).sum()
                logger.info(f"   换手率非零数量: {non_zero_count}/{len(df_basic)}")
                
                # 合并换手率数据
                df = df.merge(df_basic, on='ts_code', how='left')
                
                # 验证合并后的数据
                if 'turnover_rate' in df.columns:
                    merged_non_zero = (df['turnover_rate'] > 0).sum()
                    logger.info(f"   合并后换手率非零数量: {merged_non_zero}/{len(df)}")
            else:
                logger.warning("⚠️  未获取到每日指标数据，换手率将为 0")
                logger.warning("   可能原因:")
                logger.warning("   1. 该接口需要更高的积分权限（可能需要 2000+ 积分）")
                logger.warning("   2. 数据尚未更新")
                df['turnover_rate'] = 0.0
        except Exception as e:
            error_msg = str(e)
            logger.warning(f"⚠️  获取每日指标失败: {e}")
            
            if "没有接口访问权限" in error_msg or "权限" in error_msg:
                logger.warning("")
                logger.warning("   ⚠️  daily_basic 接口需要更高的积分权限")
                logger.warning("   当前积分: 120（基础权限）")
                logger.warning("   所需积分: 2000+（获取换手率、市盈率等指标）")
                logger.warning("")
                logger.warning("   获取积分方式:")
                logger.warning("   1. 每日签到: 每天 1 积分")
                logger.warning("   2. 分享文章: 每篇 5 积分")
                logger.warning("   3. 捐赠支持: https://tushare.pro/document/1?doc_id=13")
                logger.warning("")
                logger.warning("   或者使用其他数据源（如 AkShare）获取换手率")
            else:
                logger.warning("   可能原因:")
                logger.warning("   1. 积分不足")
                logger.warning("   2. 调用频率超限")
            
            logger.warning("")
            logger.warning("   换手率将设置为 0")
            df['turnover_rate'] = 0.0
        
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
        'turnover_rate': '换手率',  # 换手率（从 daily_basic 接口获取）
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
    
    # 6. 处理换手率（如果没有获取到，填充为 0）
    if '换手率' not in df.columns:
        df['换手率'] = 0.0
    else:
        # 填充缺失值为 0
        df['换手率'] = df['换手率'].fillna(0.0)
    
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
        local_stock_list = os.path.join(OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE)
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


def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='批量获取A股日线数据并保存为单个CSV文件')
    parser.add_argument('--token', type=str, help='TuShare API Token')
    parser.add_argument('--date', type=str, required=True, help='目标日期（格式 YYYYMMDD），必填参数')
    parser.add_argument('--codes', type=str, help='股票代码列表（逗号分隔），例如: 000001,600519,000002')
    parser.add_argument('--code', type=str, help='单个股票代码，例如: 000001')
    parser.add_argument('--output', type=str, help='输出文件名（可选），默认为: all_stocks_YYYYMMDD.csv')
    parser.add_argument('--no-filter', action='store_true', help='不过滤停牌数据（默认会过滤）')
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
        logger.error("方式1（推荐）：在 config.py 中添加")
        logger.error('  TUSHARE_TOKEN = "你的token"')
        logger.error("")
        logger.error("方式2：设置环境变量")
        logger.error('  export TUSHARE_TOKEN="你的token"')
        logger.error("")
        logger.error("方式3：命令行参数")
        logger.error('  python fetch_daily_data_tushare_to_single_file.py --token "你的token"')
        logger.error("")
        logger.error("获取 Token:")
        logger.error("  1. 注册账号: https://tushare.pro/register")
        logger.error("  2. 获取 token: https://tushare.pro/user/token")
        logger.error("="*80)
        sys.exit(1)
    
    # 获取数据
    df_raw, target_date = fetch_all_stocks_daily_data_tushare(token, args.date)
    
    if df_raw is None:
        logger.error("获取数据失败，退出")
        sys.exit(1)
    
    # 如果指定了股票代码，进行过滤
    if args.code or args.codes:
        logger.info("")
        logger.info("="*80)
        logger.info("过滤指定股票")
        logger.info("="*80)
        
        # 处理股票代码列表
        target_codes = []
        if args.code:
            target_codes.append(args.code)
        if args.codes:
            target_codes.extend([code.strip() for code in args.codes.split(',')])
        
        # 去重
        target_codes = list(set(target_codes))
        
        logger.info(f"指定的股票代码: {', '.join(target_codes)}")
        logger.info(f"原始数据总数: {len(df_raw)} 只")
        
        # 过滤数据（需要添加市场后缀）
        # 000开头 -> .SZ (深圳)
        # 600/601/603/688开头 -> .SH (上海)
        ts_codes = []
        for code in target_codes:
            if code.startswith('000') or code.startswith('001') or code.startswith('002') or code.startswith('003'):
                ts_codes.append(f"{code}.SZ")
            elif code.startswith('600') or code.startswith('601') or code.startswith('603') or code.startswith('688'):
                ts_codes.append(f"{code}.SH")
            else:
                # 如果已经包含后缀，直接使用
                if '.' in code:
                    ts_codes.append(code)
                else:
                    # 默认尝试两个市场
                    ts_codes.append(f"{code}.SZ")
                    ts_codes.append(f"{code}.SH")
        
        logger.info(f"查找的 TuShare 代码: {', '.join(ts_codes)}")
        
        # 过滤
        df_raw_filtered = df_raw[df_raw['ts_code'].isin(ts_codes)]
        
        if df_raw_filtered.empty:
            logger.error(f"未找到指定股票的数据: {', '.join(target_codes)}")
            logger.error("请检查股票代码是否正确")
            sys.exit(1)
        
        logger.info(f"过滤后数据: {len(df_raw_filtered)} 只")
        logger.info("")
        
        # 替换原始数据
        df_raw = df_raw_filtered
    
    # 保存原始数据（用于检查）
    logger.info("")
    logger.info("="*80)
    logger.info("保存原始数据")
    logger.info("="*80)
    raw_output_file = f'tushare_raw_daily_data_{target_date}.csv'
    df_raw.to_csv(raw_output_file, index=False, encoding='utf-8-sig')
    logger.info(f"✅ 原始数据已保存: {raw_output_file}")
    logger.info(f"   文件大小: {os.path.getsize(raw_output_file) / 1024:.2f} KB")
    logger.info(f"   数据行数: {len(df_raw)}")
    logger.info("")
    
    # 转换为标准格式
    logger.info("正在转换数据格式...")
    df_standard = convert_tushare_to_standard_format(df_raw, target_date)
    
    # 补充股票名称
    df_standard = supplement_stock_names(df_standard, token)
    
    # 过滤停牌数据（可选）
    if not args.no_filter:
        logger.info("")
        logger.info("正在过滤停牌数据...")
        original_count = len(df_standard)
        df_filtered, removed_count = filter_suspended_trading_data(df_standard)
        logger.info(f"✅ 过滤完成: 移除 {removed_count} 条停牌记录")
        df_standard = df_filtered
    
    # 确定输出文件名
    if args.output:
        output_file = args.output
    else:
        output_file = f'tushare_standard_daily_data_{target_date}.csv'
    
    # 保存标准格式数据
    logger.info("")
    logger.info("="*80)
    logger.info("保存标准格式数据")
    logger.info("="*80)
    df_standard.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    logger.info(f"✅ 标准格式数据已保存: {output_file}")
    logger.info(f"   文件大小: {os.path.getsize(output_file) / 1024:.2f} KB")
    logger.info(f"   数据行数: {len(df_standard)}")
    
    # 输出统计信息
    logger.info("")
    logger.info("="*80)
    logger.info("执行完成")
    logger.info("="*80)
    logger.info(f"目标日期: {target_date}")
    logger.info(f"总股票数: {len(df_standard)}")
    logger.info("")
    logger.info("输出文件:")
    logger.info(f"  1. 原始数据: {raw_output_file} ({os.path.getsize(raw_output_file) / 1024:.2f} KB)")
    logger.info(f"  2. 标准格式: {output_file} ({os.path.getsize(output_file) / 1024:.2f} KB)")
    logger.info("")
    logger.info("标准格式数据列:")
    for i, col in enumerate(df_standard.columns, 1):
        logger.info(f"  {i}. {col}")
    logger.info("="*80)


if __name__ == "__main__":
    main()
