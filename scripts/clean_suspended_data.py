#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
检查并清理所有股票文件中的停牌数据

功能：
    1. 扫描 data/CN 目录下所有股票文件
    2. 检测每个文件中是否包含停牌数据
    3. 过滤停牌数据并更新文件
    4. 生成详细的清理报告

使用方法：
    python scripts/clean_suspended_data.py
    或
    poetry run python scripts/clean_suspended_data.py
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import OUTPUT_DIR, CN_DIR
from utils import filter_suspended_trading_data, MetadataManager


def scan_and_clean_suspended_data(data_dir: str, dry_run: bool = False) -> Dict:
    """
    扫描并清理所有股票文件中的停牌数据
    
    Args:
        data_dir: 数据目录路径
        dry_run: 如果为 True，只检测不修改文件
    
    Returns:
        清理统计信息
    """
    print("=" * 80)
    print("停牌数据清理工具")
    print("=" * 80)
    print(f"数据目录: {data_dir}")
    print(f"模式: {'只检测（不修改）' if dry_run else '检测并清理'}")
    print("=" * 80)
    print()
    
    # 统计信息
    stats = {
        'total_files': 0,
        'files_with_suspended': 0,
        'files_cleaned': 0,
        'total_records_removed': 0,
        'files_with_errors': 0,
        'details': []
    }
    
    # 获取所有股票文件
    stock_files = [f for f in os.listdir(data_dir) if f.startswith('stock_') and f.endswith('.csv')]
    stats['total_files'] = len(stock_files)
    
    if stats['total_files'] == 0:
        print("⚠️  未找到任何股票文件")
        return stats
    
    print(f"找到 {stats['total_files']} 个股票文件")
    print()
    print("开始扫描...")
    print("-" * 80)
    
    # 处理每个文件
    for idx, filename in enumerate(sorted(stock_files), 1):
        filepath = os.path.join(data_dir, filename)
        stock_code = filename.replace('stock_', '').replace('.csv', '')
        
        try:
            # 读取文件
            df = pd.read_csv(filepath, dtype={'股票代码': str})
            
            if df.empty:
                continue
            
            original_count = len(df)
            
            # 检测停牌数据
            df_filtered, removed_count = filter_suspended_trading_data(df)
            
            if removed_count > 0:
                stats['files_with_suspended'] += 1
                stats['total_records_removed'] += removed_count
                
                # 记录详细信息
                detail = {
                    'stock_code': stock_code,
                    'filename': filename,
                    'original_count': original_count,
                    'removed_count': removed_count,
                    'remaining_count': len(df_filtered),
                    'removed_dates': []
                }
                
                # 找出被移除的日期（保存所有日期）
                if '日期' in df.columns:
                    removed_dates = set(df['日期']) - set(df_filtered['日期'])
                    detail['removed_dates'] = sorted(list(removed_dates))  # 保存所有日期
                
                stats['details'].append(detail)
                
                # 显示进度
                print(f"[{idx}/{stats['total_files']}] {stock_code}: 发现 {removed_count} 条停牌记录")
                
                # 如果不是 dry_run，则更新文件
                if not dry_run:
                    if not df_filtered.empty:
                        # 保存过滤后的数据
                        df_filtered.to_csv(filepath, index=False, encoding='utf-8-sig')
                        stats['files_cleaned'] += 1
                    else:
                        print(f"   ⚠️  警告：过滤后无数据，保留原文件")
            else:
                # 每100个文件显示一次进度
                if idx % 100 == 0:
                    print(f"[{idx}/{stats['total_files']}] 已处理 {idx} 个文件...")
        
        except Exception as e:
            stats['files_with_errors'] += 1
            print(f"[{idx}/{stats['total_files']}] {stock_code}: ❌ 错误 - {str(e)}")
    
    print("-" * 80)
    print()
    
    return stats


def update_metadata_after_cleaning(data_dir: str, cleaned_stocks: List[str]):
    """
    清理后更新元数据
    
    Args:
        data_dir: 数据目录
        cleaned_stocks: 已清理的股票代码列表
    """
    if not cleaned_stocks:
        return
    
    print("正在更新元数据...")
    
    try:
        metadata_mgr = MetadataManager(data_dir)
        
        for stock_code in cleaned_stocks:
            filepath = os.path.join(data_dir, f'stock_{stock_code}.csv')
            
            try:
                df = pd.read_csv(filepath, dtype={'股票代码': str})
                if not df.empty and '日期' in df.columns:
                    last_date = df['日期'].max()
                    metadata_mgr.update_last_date(stock_code, last_date)
            except Exception as e:
                print(f"   ⚠️  更新 {stock_code} 元数据失败: {e}")
        
        print("✅ 元数据更新完成")
    except Exception as e:
        print(f"⚠️  元数据更新失败: {e}")


def print_report(stats: Dict, dry_run: bool):
    """
    打印清理报告
    
    Args:
        stats: 统计信息
        dry_run: 是否为检测模式
    """
    print("=" * 80)
    print("清理报告")
    print("=" * 80)
    print(f"总文件数: {stats['total_files']}")
    print(f"包含停牌数据的文件: {stats['files_with_suspended']}")
    
    if not dry_run:
        print(f"已清理的文件: {stats['files_cleaned']}")
    
    print(f"总共移除记录数: {stats['total_records_removed']}")
    print(f"处理出错的文件: {stats['files_with_errors']}")
    print()
    
    if stats['files_with_suspended'] > 0:
        print("详细信息（前20个，完整列表见报告文件）：")
        print("-" * 80)
        
        for detail in stats['details'][:20]:
            print(f"\n股票代码: {detail['stock_code']}")
            print(f"  原始记录: {detail['original_count']} 条")
            print(f"  移除记录: {detail['removed_count']} 条")
            print(f"  剩余记录: {detail['remaining_count']} 条")
            
            if detail['removed_dates']:
                # 在控制台只显示前5个日期
                dates_str = ', '.join(detail['removed_dates'][:5])
                if len(detail['removed_dates']) > 5:
                    dates_str += f" ... (共 {len(detail['removed_dates'])} 个日期，完整列表见报告文件)"
                print(f"  停牌日期: {dates_str}")
        
        if len(stats['details']) > 20:
            print(f"\n... 还有 {len(stats['details']) - 20} 个文件包含停牌数据（完整列表见报告文件）")
    
    print()
    print("=" * 80)
    
    if dry_run:
        print("💡 这是检测模式，未修改任何文件")
        print("   如需清理，请运行: python scripts/clean_suspended_data.py --clean")
    else:
        print("✅ 清理完成！")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='检查并清理所有股票文件中的停牌数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s                    # 只检测，不修改文件
  %(prog)s --clean            # 检测并清理
  %(prog)s --clean --backup   # 清理前备份（暂未实现）
"""
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='执行清理操作（默认只检测）'
    )
    
    args = parser.parse_args()
    
    # 构建数据目录路径
    data_dir = os.path.join(OUTPUT_DIR, CN_DIR)
    
    if not os.path.exists(data_dir):
        print(f"❌ 数据目录不存在: {data_dir}")
        sys.exit(1)
    
    # 执行扫描和清理
    dry_run = not args.clean
    stats = scan_and_clean_suspended_data(data_dir, dry_run=dry_run)
    
    # 如果执行了清理，更新元数据
    if args.clean and stats['files_cleaned'] > 0:
        cleaned_stocks = [d['stock_code'] for d in stats['details']]
        update_metadata_after_cleaning(data_dir, cleaned_stocks)
    
    # 打印报告
    print_report(stats, dry_run)
    
    # 保存详细报告到文件
    if stats['files_with_suspended'] > 0:
        report_file = os.path.join(data_dir, f"suspended_data_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("停牌数据清理报告\n")
                f.write("=" * 80 + "\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"模式: {'只检测' if dry_run else '检测并清理'}\n")
                f.write(f"总文件数: {stats['total_files']}\n")
                f.write(f"包含停牌数据的文件: {stats['files_with_suspended']}\n")
                f.write(f"总共移除记录数: {stats['total_records_removed']}\n")
                f.write("\n详细信息:\n")
                f.write("-" * 80 + "\n")
                
                for detail in stats['details']:
                    f.write(f"\n股票代码: {detail['stock_code']}\n")
                    f.write(f"  原始记录: {detail['original_count']} 条\n")
                    f.write(f"  移除记录: {detail['removed_count']} 条\n")
                    f.write(f"  剩余记录: {detail['remaining_count']} 条\n")
                    
                    if detail['removed_dates']:
                        f.write(f"  停牌日期: {', '.join(detail['removed_dates'])}\n")
            
            print(f"\n详细报告已保存到: {report_file}")
        
        except Exception as e:
            print(f"\n⚠️  保存报告失败: {e}")


if __name__ == "__main__":
    main()
