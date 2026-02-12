#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
获取单只股票历史数据脚本
支持多数据源自动切换，简单易用

特点：
    ✅ 全量替换模式（不做增量更新，直接覆盖）
    ✅ 自动更新元数据（确保其他脚本能正确识别最新日期）
    ✅ 自动过滤停牌数据
    ✅ 支持多数据源自动切换

使用方法：
    方式1（推荐）：
        python fetch_single_stock.py 000001
        python fetch_single_stock.py 000001 --start 20240101 --end 20240131
    
    方式2：
        poetry run python fetch_single_stock.py 600519
    
    方式3：交互式输入
        python fetch_single_stock.py

参数说明：
    stock_code: 股票代码（6位，如 000001、600519）
    --start: 开始日期（格式：YYYYMMDD，默认：20000101）
    --end: 结束日期（格式：YYYYMMDD，默认：昨天）
    --adjust: 复权类型（qfq=前复权, hfq=后复权, ''=不复权，默认：qfq）
    --output: 输出文件路径（默认：data/CN/stock_{代码}.csv）

示例：
    # 获取平安银行全部历史数据
    python fetch_single_stock.py 000001
    
    # 获取贵州茅台 2024年1月的数据
    python fetch_single_stock.py 600519 --start 20240101 --end 20240131
    
    # 获取数据并保存到指定位置
    python fetch_single_stock.py 000001 --output my_data.csv
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE, ADJUST_TYPE
from fetchers import MultiSourceFetcher
from utils import save_dataframe, get_safe_end_date, filter_suspended_trading_data, MetadataManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)


def get_stock_name(stock_code: str) -> str:
    """
    从股票列表中获取股票名称
    
    Args:
        stock_code: 股票代码
    
    Returns:
        股票名称，如果找不到则返回代码本身
    """
    try:
        import pandas as pd
        stock_list_file = os.path.join(OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE)
        
        if os.path.exists(stock_list_file):
            df_list = pd.read_csv(stock_list_file, dtype={'code': str})
            df_list['code'] = df_list['code'].astype(str).str.zfill(6)
            
            match = df_list[df_list['code'] == stock_code]
            if not match.empty:
                return match.iloc[0]['name']
    except Exception:
        pass
    
    return stock_code


def fetch_single_stock(
    stock_code: str,
    start_date: str = "20000101",
    end_date: str = None,
    adjust_type: str = "qfq",
    output_file: str = None,
    preferred_source: str = "baostock"
) -> bool:
    """
    获取单只股票的历史数据（全量替换模式）
    
    注意：
    - 直接全量替换，不做增量更新
    - 自动更新元数据（确保与其他脚本的兼容性）
    - 适合单次查询或测试使用
    
    Args:
        stock_code: 股票代码（6位）
        start_date: 开始日期（格式：YYYYMMDD）
        end_date: 结束日期（格式：YYYYMMDD，None表示昨天）
        adjust_type: 复权类型（qfq/hfq/''）
        output_file: 输出文件路径（None表示使用默认路径）
        preferred_source: 优先数据源（baostock/akshare/yfinance，默认：baostock）
    
    Returns:
        是否成功获取数据
    """
    # 确保股票代码是6位
    stock_code = stock_code.zfill(6)
    
    # 获取股票名称
    stock_name = get_stock_name(stock_code)
    
    # 处理结束日期
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    # 应用18:30时间检查
    safe_end_date, date_adjusted = get_safe_end_date(end_date)
    if date_adjusted:
        end_date = safe_end_date
    
    # 确定输出文件路径
    if output_file is None:
        cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
        os.makedirs(cn_dir, exist_ok=True)
        output_file = os.path.join(cn_dir, f"stock_{stock_code}.csv")
    else:
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 80)
    print(f"获取股票历史数据")
    print("=" * 80)
    print(f"股票代码: {stock_code}")
    print(f"股票名称: {stock_name}")
    print(f"日期范围: {start_date} ~ {end_date}")
    print(f"复权类型: {adjust_type}")
    print(f"优先数据源: {preferred_source}")
    print(f"输出文件: {output_file}")
    print("=" * 80)
    print()
    
    # 获取数据
    print(f"正在从 {preferred_source} 获取数据（失败时自动切换其他数据源）...")
    
    with MultiSourceFetcher(preferred_source=preferred_source) as fetcher:
        result = fetcher.fetch(
            stock_code=stock_code,
            stock_name=stock_name,
            start_date=start_date,
            end_date=end_date,
            adjust_type=adjust_type
        )
        
        if result.data is None:
            if result.source == "no_data":
                print(f"❌ 无数据：该时间段内没有交易数据（可能是节假日/停牌/未上市）")
            else:
                print(f"❌ 获取失败：所有数据源均失败")
            return False
        
        df = result.data
        source = result.source
        
        print(f"✅ 获取成功！数据来源：{source}")
        print(f"   原始数据: {len(df)} 条记录")
        
        # 过滤停牌数据
        df_filtered, removed_count = filter_suspended_trading_data(df)
        
        if removed_count > 0:
            print(f"   过滤停牌: 移除 {removed_count} 条停牌记录")
        
        if df_filtered.empty:
            print(f"❌ 过滤后无有效数据（全部为停牌数据）")
            # 仍需更新元数据，避免下次重复拉取
            try:
                cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
                metadata_mgr = MetadataManager(cn_dir)
                metadata_mgr.update_last_date(stock_code, end_date)
                print(f"✅ 元数据已更新：最新日期 = {end_date}")
            except Exception as e:
                print(f"⚠️  元数据更新失败: {e}")
            return False
        
        print(f"   有效数据: {len(df_filtered)} 条记录")
        print()
        
        # 保存数据（直接全量替换，不做增量更新）
        print(f"正在保存到：{output_file}")
        save_dataframe(df_filtered, output_file, stock_code)
        print(f"✅ 保存成功！（全量替换模式）")
        
        # 更新元数据（确保其他脚本能正确识别最新日期）
        # 使用请求的结束日期，而非CSV中的最后日期
        try:
            cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
            metadata_mgr = MetadataManager(cn_dir)
            metadata_mgr.update_last_date(stock_code, end_date)
            print(f"✅ 元数据已更新：最新日期 = {end_date}")
        except Exception as e:
            print(f"⚠️  元数据更新失败（不影响数据保存）: {e}")
        
        print()
        
        # 显示数据预览
        print("数据预览（前5条）：")
        print("-" * 80)
        # 选择要显示的列
        display_cols = ['日期', '股票名称', '开盘', '收盘', '最高', '最低', '成交量', '涨跌幅']
        available_cols = [col for col in display_cols if col in df_filtered.columns]
        print(df_filtered[available_cols].head(5).to_string(index=False))
        print("-" * 80)
        print()
        
        # 显示统计信息
        print("数据统计：")
        print(f"  起始日期: {df_filtered['日期'].iloc[0]}")
        print(f"  结束日期: {df_filtered['日期'].iloc[-1]}")
        print(f"  记录条数: {len(df_filtered)}")
        print(f"  最高价: {df_filtered['最高'].max():.2f}")
        print(f"  最低价: {df_filtered['最低'].min():.2f}")
        print(f"  平均成交量: {df_filtered['成交量'].mean():.0f}")
        print()
        
        return True


def interactive_mode():
    """交互式模式"""
    print("=" * 80)
    print("单只股票历史数据获取工具（交互式）")
    print("=" * 80)
    print()
    
    # 输入股票代码
    while True:
        stock_code = input("请输入股票代码（6位，如 000001）: ").strip()
        if len(stock_code) == 6 and stock_code.isdigit():
            break
        print("❌ 股票代码格式错误，请输入6位数字")
    
    # 输入日期范围
    print()
    print("日期范围（直接回车使用默认值）：")
    start_date = input(f"  开始日期 [默认: 20000101]: ").strip() or "20000101"
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    end_date = input(f"  结束日期 [默认: {yesterday}]: ").strip() or yesterday
    
    # 输入复权类型
    print()
    adjust_type = input("复权类型 [qfq=前复权, hfq=后复权, ''=不复权, 默认: qfq]: ").strip() or "qfq"
    
    # 输入数据源
    print()
    preferred_source = input("优先数据源 [baostock/akshare/yfinance, 默认: baostock]: ").strip() or "baostock"
    
    print()
    
    # 执行获取
    fetch_single_stock(stock_code, start_date, end_date, adjust_type, preferred_source=preferred_source)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='获取单只股票历史数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s 000001                                    # 获取平安银行全部历史数据
  %(prog)s 600519 --start 20240101 --end 20240131   # 获取贵州茅台2024年1月数据
  %(prog)s 000001 --output my_data.csv              # 保存到指定文件
  %(prog)s 000001 --source akshare                  # 使用akshare作为优先数据源
  %(prog)s                                          # 交互式输入
"""
    )
    
    parser.add_argument(
        'stock_code',
        nargs='?',
        help='股票代码（6位，如 000001、600519）'
    )
    parser.add_argument(
        '--start',
        default='20000101',
        help='开始日期（格式：YYYYMMDD，默认：20000101）'
    )
    parser.add_argument(
        '--end',
        default=None,
        help='结束日期（格式：YYYYMMDD，默认：昨天）'
    )
    parser.add_argument(
        '--adjust',
        default='qfq',
        choices=['qfq', 'hfq', ''],
        help='复权类型（qfq=前复权, hfq=后复权, \'\'=不复权，默认：qfq）'
    )
    parser.add_argument(
        '--output',
        default=None,
        help='输出文件路径（默认：data/CN/stock_{代码}.csv）'
    )
    parser.add_argument(
        '--source',
        default='baostock',
        choices=['baostock', 'akshare', 'yfinance'],
        help='优先数据源（baostock=BaoStock, akshare=AkShare, yfinance=YFinance，默认：baostock）'
    )
    
    args = parser.parse_args()
    
    # 如果没有提供股票代码，进入交互式模式
    if args.stock_code is None:
        interactive_mode()
    else:
        success = fetch_single_stock(
            stock_code=args.stock_code,
            start_date=args.start,
            end_date=args.end,
            adjust_type=args.adjust,
            output_file=args.output,
            preferred_source=args.source
        )
        
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
