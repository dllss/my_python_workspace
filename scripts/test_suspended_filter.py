#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试停牌数据过滤功能
验证成交量和成交额为空或0的数据是否被正确过滤
"""

import os
import sys
import pandas as pd
import tempfile
import shutil

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import filter_suspended_trading_data, merge_and_save_data, MetadataManager


def test_suspended_filter():
    """测试停牌数据过滤功能"""
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    
    try:
        print("=" * 80)
        print("测试：停牌数据过滤功能")
        print("=" * 80)
        
        # 1. 创建包含停牌数据的测试数据
        print("\n1️⃣  创建测试数据（包含正常交易和停牌数据）")
        df_test = pd.DataFrame({
            '日期': [
                '2026-01-08', '2026-01-09',  # 正常交易
                '2026-01-12', '2026-01-13', '2026-01-14',  # 停牌（成交量和成交额为空）
                '2026-01-15', '2026-01-16',  # 停牌
                '2026-01-26', '2026-01-27'   # 恢复交易
            ],
            '股票名称': ['湖南黄金'] * 9,
            '股票代码': ['002155'] * 9,
            '开盘': [22.26, 22.09, 22.97, 22.97, 22.97, 22.97, 22.97, 25.27, 27.8],
            '收盘': [22.1, 22.97, 22.97, 22.97, 22.97, 22.97, 22.97, 25.27, 27.8],
            '最高': [22.68, 23.05, 22.97, 22.97, 22.97, 22.97, 22.97, 25.27, 27.8],
            '最低': [21.87, 22.05, 22.97, 22.97, 22.97, 22.97, 22.97, 25.27, 27.8],
            '成交量': [42007819.0, 63745873.0, None, None, None, None, None, 7963510.0, 3041605.0],
            '成交额': [932744590.11, 1451856904.2, None, None, None, None, None, 201237897.7, 84556619.0],
            '振幅': [3.63, 4.52, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            '涨跌幅': [-0.99, 3.94, None, None, None, None, None, 10.01, 10.01],
            '涨跌额': [-0.22, 0.87, 0.0, 0.0, 0.0, 0.0, 0.0, 2.3, 2.53],
            '换手率': [2.69, 4.08, None, None, None, None, None, 0.51, 0.19]
        })
        
        print(f"   原始数据: {len(df_test)} 条记录")
        print(f"   - 正常交易: 4 条 (2026-01-08, 09, 26, 27)")
        print(f"   - 停牌数据: 5 条 (2026-01-12~16)")
        
        # 2. 测试过滤功能
        print("\n2️⃣  执行停牌数据过滤")
        df_filtered, removed_count = filter_suspended_trading_data(df_test)
        
        print(f"   ✅ 过滤完成")
        print(f"   - 保留记录: {len(df_filtered)} 条")
        print(f"   - 移除记录: {removed_count} 条")
        
        # 3. 验证结果
        print("\n3️⃣  验证过滤结果")
        
        # 检查是否只保留了有成交量和成交额的数据
        expected_dates = ['2026-01-08', '2026-01-09', '2026-01-26', '2026-01-27']
        actual_dates = df_filtered['日期'].tolist()
        
        if actual_dates == expected_dates:
            print("   ✅ 测试通过！停牌数据已被正确过滤")
            print(f"\n   保留的交易日期:")
            for date in actual_dates:
                row = df_filtered[df_filtered['日期'] == date].iloc[0]
                print(f"      {date}: 成交量={row['成交量']:.0f}, 成交额={row['成交额']:.2f}")
            
            print(f"\n   被过滤的停牌日期:")
            suspended_dates = ['2026-01-12', '2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16']
            for date in suspended_dates:
                print(f"      {date}: 成交量=空, 成交额=空")
            
            return True
        else:
            print(f"   ❌ 测试失败！")
            print(f"   预期日期: {expected_dates}")
            print(f"   实际日期: {actual_dates}")
            return False
            
    finally:
        # 清理临时目录
        shutil.rmtree(temp_dir)
        print("\n✅ 临时文件已清理")


def test_merge_with_filter():
    """测试合并数据时的停牌过滤"""
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        print("\n" + "=" * 80)
        print("测试：合并数据时过滤停牌记录")
        print("=" * 80)
        
        # 1. 创建初始数据（包含停牌）
        print("\n1️⃣  创建初始数据（包含停牌记录）")
        df_initial = pd.DataFrame({
            '日期': ['2026-01-08', '2026-01-09', '2026-01-12', '2026-01-13'],
            '股票名称': ['湖南黄金'] * 4,
            '股票代码': ['002155'] * 4,
            '开盘': [22.26, 22.09, 22.97, 22.97],
            '收盘': [22.1, 22.97, 22.97, 22.97],
            '最高': [22.68, 23.05, 22.97, 22.97],
            '最低': [21.87, 22.05, 22.97, 22.97],
            '成交量': [42007819.0, 63745873.0, None, None],
            '成交额': [932744590.11, 1451856904.2, None, None],
            '振幅': [3.63, 4.52, 0.0, 0.0],
            '涨跌幅': [-0.99, 3.94, None, None],
            '涨跌额': [-0.22, 0.87, 0.0, 0.0],
            '换手率': [2.69, 4.08, None, None]
        })
        
        output_file = os.path.join(temp_dir, "stock_002155.csv")
        metadata_mgr = MetadataManager(temp_dir)
        
        # 保存初始数据
        is_update, new_count, removed_count = merge_and_save_data(df_initial, output_file, '002155', False, metadata_mgr)
        
        # 读取并验证
        df_saved = pd.read_csv(output_file)
        print(f"   ✅ 初始数据已保存")
        print(f"   - 原始: 4 条记录（2条正常 + 2条停牌）")
        print(f"   - 保存: {len(df_saved)} 条记录（过滤{removed_count}条停牌）")
        
        # 2. 添加新数据（包含停牌和正常交易）
        print("\n2️⃣  添加新数据（包含停牌和恢复交易）")
        df_new = pd.DataFrame({
            '日期': ['2026-01-14', '2026-01-15', '2026-01-26'],
            '股票名称': ['湖南黄金'] * 3,
            '股票代码': ['002155'] * 3,
            '开盘': [22.97, 22.97, 25.27],
            '收盘': [22.97, 22.97, 25.27],
            '最高': [22.97, 22.97, 25.27],
            '最低': [22.97, 22.97, 25.27],
            '成交量': [None, None, 7963510.0],
            '成交额': [None, None, 201237897.7],
            '振幅': [0.0, 0.0, 0.0],
            '涨跌幅': [None, None, 10.01],
            '涨跌额': [0.0, 0.0, 2.3],
            '换手率': [None, None, 0.51]
        })
        
        is_update, new_count, removed_count = merge_and_save_data(df_new, output_file, '002155', False, metadata_mgr)
        
        # 读取并验证
        df_final = pd.read_csv(output_file)
        print(f"   ✅ 数据已更新")
        print(f"   - 新增: 3 条记录（2条停牌 + 1条正常）")
        print(f"   - 最终: {len(df_final)} 条记录（只保留正常交易）")
        
        # 3. 验证最终结果
        print("\n3️⃣  验证最终结果")
        expected_count = 3  # 只有3条正常交易数据
        
        if len(df_final) == expected_count:
            print(f"   ✅ 测试通过！最终保存了 {expected_count} 条正常交易数据")
            print(f"\n   保存的交易日期:")
            for _, row in df_final.iterrows():
                print(f"      {row['日期']}: 成交量={row['成交量']:.0f}")
            return True
        else:
            print(f"   ❌ 测试失败！")
            print(f"   预期: {expected_count} 条记录")
            print(f"   实际: {len(df_final)} 条记录")
            return False
            
    finally:
        shutil.rmtree(temp_dir)
        print("\n✅ 临时文件已清理")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("停牌数据过滤功能测试")
    print("=" * 80)
    
    # 测试1：基本过滤功能
    success1 = test_suspended_filter()
    
    # 测试2：合并时过滤
    success2 = test_merge_with_filter()
    
    print("\n" + "=" * 80)
    if success1 and success2:
        print("✅ 所有测试通过！")
        print("\n功能说明：")
        print("  - 自动过滤成交量或成交额为空/0的停牌数据")
        print("  - 在保存和合并数据时都会执行过滤")
        print("  - 确保CSV文件中只包含正常交易数据")
    else:
        print("❌ 部分测试失败！")
        sys.exit(1)
    print("=" * 80 + "\n")
