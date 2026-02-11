#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分析股票名称变化历史
检测股票何时改名（如变为 ST、退市等）
"""

import os
import sys
import pandas as pd
from datetime import datetime

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_DIR, CN_DIR


def analyze_stock_name_changes(stock_code: str = None, show_all: bool = False):
    """
    分析股票名称变化历史
    
    Args:
        stock_code: 指定股票代码（可选，如果不指定则分析所有股票）
        show_all: 是否显示所有股票（包括没有改名的）
    """
    cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
    
    if not os.path.exists(cn_dir):
        print(f"❌ 数据目录不存在: {cn_dir}")
        return
    
    # 获取要分析的股票列表
    if stock_code:
        stock_files = [f"stock_{stock_code}.csv"]
    else:
        stock_files = [f for f in os.listdir(cn_dir) if f.startswith('stock_') and f.endswith('.csv')]
    
    print("=" * 80)
    print("股票名称变化历史分析")
    print("=" * 80)
    print(f"分析范围: {len(stock_files)} 只股票")
    print()
    
    # 统计信息
    total_stocks = 0
    changed_stocks = 0
    st_stocks = []
    delisted_stocks = []
    
    for stock_file in sorted(stock_files):
        file_path = os.path.join(cn_dir, stock_file)
        
        try:
            df = pd.read_csv(file_path, dtype={'股票代码': str})
            
            if '股票名称' not in df.columns or df.empty:
                continue
            
            total_stocks += 1
            stock_code_val = df['股票代码'].iloc[0]
            
            # 获取名称变化历史
            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期')
            
            # 检测名称变化
            name_changes = []
            prev_name = None
            
            for idx, row in df.iterrows():
                current_name = row['股票名称']
                current_date = row['日期'].strftime('%Y-%m-%d')
                
                if prev_name is None:
                    # 第一条记录
                    name_changes.append({
                        'date': current_date,
                        'name': current_name,
                        'is_first': True
                    })
                elif current_name != prev_name:
                    # 名称发生变化
                    name_changes.append({
                        'date': current_date,
                        'name': current_name,
                        'is_first': False,
                        'prev_name': prev_name
                    })
                
                prev_name = current_name
            
            # 判断是否有名称变化
            has_changes = len(name_changes) > 1
            
            if has_changes:
                changed_stocks += 1
                
                # 检查是否变为 ST 或退市
                latest_name = name_changes[-1]['name']
                if 'ST' in latest_name or 'st' in latest_name:
                    st_stocks.append((stock_code_val, latest_name))
                if '退市' in latest_name:
                    delisted_stocks.append((stock_code_val, latest_name))
                
                # 显示变化历史
                print(f"📊 {stock_code_val} - 名称变化历史:")
                for change in name_changes:
                    if change['is_first']:
                        print(f"   {change['date']}: 初始名称 = {change['name']}")
                    else:
                        print(f"   {change['date']}: {change['prev_name']} → {change['name']}")
                print()
            
            elif show_all:
                # 显示没有改名的股票
                print(f"✅ {stock_code_val} - {name_changes[0]['name']} (无改名)")
        
        except Exception as e:
            print(f"❌ 分析 {stock_file} 失败: {str(e)}")
            continue
    
    # 显示统计信息
    print("=" * 80)
    print("统计摘要")
    print("=" * 80)
    print(f"总股票数: {total_stocks}")
    print(f"有改名的股票: {changed_stocks} ({changed_stocks/total_stocks*100:.1f}%)")
    print(f"当前为 ST 的股票: {len(st_stocks)}")
    print(f"已退市的股票: {len(delisted_stocks)}")
    
    if st_stocks:
        print("\n⚠️  当前 ST 股票列表:")
        for code, name in st_stocks[:10]:  # 只显示前10个
            print(f"   {code}: {name}")
        if len(st_stocks) > 10:
            print(f"   ... 还有 {len(st_stocks) - 10} 只")
    
    if delisted_stocks:
        print("\n🚫 已退市股票列表:")
        for code, name in delisted_stocks:
            print(f"   {code}: {name}")
    
    print("=" * 80)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='分析股票名称变化历史')
    parser.add_argument('--code', type=str, help='指定股票代码（6位）')
    parser.add_argument('--all', action='store_true', help='显示所有股票（包括没有改名的）')
    
    args = parser.parse_args()
    
    analyze_stock_name_changes(stock_code=args.code, show_all=args.all)


if __name__ == "__main__":
    main()
