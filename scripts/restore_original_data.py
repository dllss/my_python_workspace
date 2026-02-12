#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
重新获取原始数据（包含停牌数据）

此脚本用于重新获取包含停牌数据的原始股票数据，
以便生成完整的停牌日期列表报告

使用方法：
    python scripts/restore_original_data.py
    或
    poetry run python scripts/restore_original_data.py

注意：
    - 此脚本会重新获取所有股票的历史数据
    - 不会过滤停牌数据
    - 建议先备份当前数据
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import time
import random

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE, START_DATE, DELAY_MIN, DELAY_MAX
from fetchers import MultiSourceFetcher
from utils import save_dataframe, get_safe_end_date


def restore_original_data_for_stock(stock_code: str, stock_name: str, output_dir: str):
    """
    重新获取单只股票的原始数据（不过滤停牌数据）
    
    Args:
        stock_code: 股票代码
        stock_name: 股票名称
        output_dir: 输出目录
    
    Returns:
        是否成功
    """
    output_file = os.path.join(output_dir, f'stock_{stock_code}.csv')
    
    # 获取安全的结束日期（昨天）
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    print(f"正在获取 {stock_code} {stock_name} 的原始数据...")
    
    try:
        with MultiSourceFetcher() as fetcher:
            result = fetcher.fetch(
                stock_code=stock_code,
                stock_name=stock_name,
                start_date=START_DATE,
                end_date=end_date,
                adjust_type='qfq'
            )
            
            if result.data is None:
                print(f"  ❌ 获取失败")
                return False
            
            df = result.data
            
            # 不过滤停牌数据，直接保存
            save_dataframe(df, output_file, stock_code)
            
            print(f"  ✅ 成功获取 {len(df)} 条记录（包含停牌数据）")
            return True
    
    except Exception as e:
        print(f"  ❌ 错误: {str(e)}")
        return False


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='重新获取原始数据（包含停牌数据）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                    # 重新获取所有股票数据
  %(prog)s --code 000001      # 只重新获取指定股票
  %(prog)s --backup           # 先备份当前数据
"""
    )
    
    parser.add_argument(
        '--code',
        help='只重新获取指定股票代码'
    )
    
    parser.add_argument(
        '--backup',
        action='store_true',
        help='先备份当前数据'
    )
    
    args = parser.parse_args()
    
    # 构建数据目录路径
    data_dir = os.path.join(OUTPUT_DIR, CN_DIR)
    stock_list_file = os.path.join(data_dir, STOCK_LIST_FILE)
    
    if not os.path.exists(stock_list_file):
        print(f"❌ 股票列表文件不存在: {stock_list_file}")
        print("   请先运行: make list")
        sys.exit(1)
    
    # 备份当前数据
    if args.backup:
        backup_dir = f"{data_dir}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"正在备份当前数据到: {backup_dir}")
        
        import shutil
        try:
            shutil.copytree(data_dir, backup_dir)
            print(f"✅ 备份完成")
        except Exception as e:
            print(f"❌ 备份失败: {e}")
            sys.exit(1)
    
    print("=" * 80)
    print("重新获取原始数据（包含停牌数据）")
    print("=" * 80)
    print()
    
    # 读取股票列表
    stock_list = pd.read_csv(stock_list_file, dtype={'code': str})
    
    if args.code:
        # 只处理指定股票
        stock_list = stock_list[stock_list['code'] == args.code]
        if stock_list.empty:
            print(f"❌ 未找到股票代码: {args.code}")
            sys.exit(1)
    
    print(f"需要处理的股票数量: {len(stock_list)}")
    print()
    
    # 统计信息
    success_count = 0
    failed_count = 0
    
    # 处理每只股票
    for idx, (_, row) in enumerate(stock_list.iterrows(), 1):
        stock_code = row['code']
        stock_name = row['name']
        
        print(f"[{idx}/{len(stock_list)}] ", end='')
        
        if restore_original_data_for_stock(stock_code, stock_name, data_dir):
            success_count += 1
        else:
            failed_count += 1
        
        # 延迟
        if idx < len(stock_list):
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            time.sleep(delay)
    
    print()
    print("=" * 80)
    print("完成！")
    print("=" * 80)
    print(f"成功: {success_count}")
    print(f"失败: {failed_count}")
    print()
    print("下一步：运行 make clean-suspended 生成完整的停牌日期报告")
    print("=" * 80)


if __name__ == "__main__":
    main()
