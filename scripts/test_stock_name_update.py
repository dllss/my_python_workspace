#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试股票名称更新功能
验证当股票改名（如变为 ST）时，历史数据中的名称是否会统一更新
"""

import os
import sys
import pandas as pd
import tempfile
import shutil

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import merge_and_save_data, MetadataManager


def test_stock_name_update():
    """测试股票名称更新功能"""
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    
    try:
        print("=" * 60)
        print("测试：股票名称更新功能")
        print("=" * 60)
        
        # 1. 创建初始数据（股票名称：乐视网）
        print("\n1️⃣  创建初始数据（股票名称：乐视网）")
        df_initial = pd.DataFrame({
            '日期': ['2024-01-01', '2024-01-02', '2024-01-03'],
            '股票名称': ['乐视网', '乐视网', '乐视网'],
            '股票代码': ['300104', '300104', '300104'],
            '开盘': [10.0, 10.5, 11.0],
            '收盘': [10.5, 11.0, 11.5],
            '最高': [10.8, 11.2, 11.8],
            '最低': [9.8, 10.2, 10.8],
            '成交量': [1000000, 1100000, 1200000],
            '成交额': [10500000, 11550000, 13800000],
            '振幅': [10.0, 9.52, 9.09],
            '涨跌幅': [5.0, 4.76, 4.55],
            '涨跌额': [0.5, 0.5, 0.5],
            '换手率': [5.0, 5.5, 6.0]
        })
        
        output_file = os.path.join(temp_dir, "stock_300104.csv")
        metadata_mgr = MetadataManager(temp_dir)
        
        # 保存初始数据
        is_update, new_count, removed_count = merge_and_save_data(df_initial, output_file, '300104', False, metadata_mgr)
        
        # 读取并验证
        df_saved = pd.read_csv(output_file)
        print(f"   ✅ 初始数据已保存，共 {len(df_saved)} 条记录")
        print(f"   股票名称: {df_saved['股票名称'].unique().tolist()}")
        
        # 2. 添加新数据（股票名称变为：*ST乐视）
        print("\n2️⃣  添加新数据（股票名称变为：*ST乐视）")
        df_new = pd.DataFrame({
            '日期': ['2024-06-01', '2024-06-02'],
            '股票名称': ['*ST乐视', '*ST乐视'],
            '股票代码': ['300104', '300104'],
            '开盘': [5.0, 5.2],
            '收盘': [5.2, 5.4],
            '最高': [5.5, 5.6],
            '最低': [4.8, 5.0],
            '成交量': [500000, 520000],
            '成交额': [2600000, 2808000],
            '振幅': [13.46, 11.54],
            '涨跌幅': [4.0, 3.85],
            '涨跌额': [0.2, 0.2],
            '换手率': [2.5, 2.6]
        })
        
        # 合并数据（保留历史名称策略）
        is_update, new_count, removed_count = merge_and_save_data(df_new, output_file, '300104', False, metadata_mgr)
        
        # 读取并验证
        df_updated = pd.read_csv(output_file)
        print(f"   ✅ 数据已更新，共 {len(df_updated)} 条记录")
        print(f"   股票名称: {df_updated['股票名称'].unique().tolist()}")
        
        # 3. 验证历史名称是否被保留
        print("\n3️⃣  验证结果（保留历史名称策略）")
        unique_names = df_updated['股票名称'].unique()
        
        # 检查名称变化历史
        name_changes = []
        prev_name = None
        for idx, row in df_updated.iterrows():
            current_name = row['股票名称']
            if prev_name is None or current_name != prev_name:
                name_changes.append({
                    'date': row['日期'],
                    'name': current_name
                })
                prev_name = current_name
        
        if len(unique_names) == 2 and '乐视网' in unique_names and '*ST乐视' in unique_names:
            print("   ✅ 测试通过！历史名称已保留，可以追踪名称变化")
            print(f"   发现 {len(unique_names)} 个不同的股票名称: {unique_names.tolist()}")
            print(f"\n   名称变化历史:")
            for change in name_changes:
                print(f"      {change['date']}: {change['name']}")
            
            # 显示部分数据
            print("\n   数据预览（前3条和后2条）:")
            print(df_updated[['日期', '股票名称', '股票代码', '收盘']].head(3).to_string(index=False))
            print("   ...")
            print(df_updated[['日期', '股票名称', '股票代码', '收盘']].tail(2).to_string(index=False))
            
            return True
        else:
            print(f"   ❌ 测试失败！")
            print(f"   预期: 保留历史名称 ['乐视网', '*ST乐视']")
            print(f"   实际: {unique_names.tolist()}")
            return False
            
    finally:
        # 清理临时目录
        shutil.rmtree(temp_dir)
        print("\n✅ 临时文件已清理")


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("股票名称更新功能测试")
    print("=" * 60)
    
    success = test_stock_name_update()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ 所有测试通过！")
        print("\n功能说明（保留历史名称策略）：")
        print("  - 当股票改名（如变为 ST、退市等）时")
        print("  - 系统会保留历史数据中的原始名称")
        print("  - 新增数据使用最新的股票名称")
        print("  - 这样可以追踪股票名称的变化历史，知道何时改名")
        print("\n优势：")
        print("  ✅ 真实反映历史状态")
        print("  ✅ 可以追踪名称变化时间点")
        print("  ✅ 数据完整性高")
        print("\n使用 analyze_stock_name_changes.py 可以分析名称变化历史")
    else:
        print("❌ 测试失败！")
        sys.exit(1)
    print("=" * 60 + "\n")
