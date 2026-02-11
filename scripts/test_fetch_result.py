#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试 FetchResult 命名元组的使用
演示如何使用命名元组访问返回结果
"""

from fetchers import MultiSourceFetcher

print("=" * 60)
print("测试 FetchResult 命名元组")
print("=" * 60)
print()

with MultiSourceFetcher() as fetcher:
    # 测试1：成功获取数据
    print("测试 1: 获取交易日数据")
    print("-" * 60)
    result = fetcher.fetch(
        stock_code='000001',
        stock_name='平安银行',
        start_date='20260210',
        end_date='20260210',
        adjust_type='qfq'
    )
    
    print(f"result.data 类型: {type(result.data)}")
    print(f"result.source: {result.source}")
    
    if result.data is not None:
        print(f"✅ 成功获取 {len(result.data)} 条数据")
        print(f"数据来源: {result.source}")
        print("\n数据预览:")
        print(result.data.head())
    elif result.source == "no_data":
        print("⏭️  无数据（节假日/停牌等）")
    else:
        print("❌ 所有数据源均失败")
    
    print()
    
    # 测试2：周末查询（应该返回 holiday）
    print("测试 2: 查询周末数据（2026-02-08 是周六）")
    print("-" * 60)
    result = fetcher.fetch(
        stock_code='000001',
        stock_name='平安银行',
        start_date='20260208',
        end_date='20260208',
        adjust_type='qfq'
    )
    
    print(f"result.data: {result.data}")
    print(f"result.source: {result.source}")
    
    if result.data is not None:
        print(f"✅ 成功获取 {len(result.data)} 条数据")
    elif result.source == "no_data":
        print("⏭️  无数据（节假日/周末/停牌，符合预期）")
    else:
        print("❌ 所有数据源均失败")
    
    print()
    
    # 测试3：使用属性访问（更清晰）
    print("测试 3: 使用属性名访问（推荐）")
    print("-" * 60)
    result = fetcher.fetch(
        stock_code='600519',
        stock_name='贵州茅台',
        start_date='20260210',
        end_date='20260210',
        adjust_type='qfq'
    )
    
    # 使用属性名访问，代码更清晰
    if result.data is not None:
        print(f"✅ 数据源: result.source = '{result.source}'")
        print(f"✅ 数据行数: len(result.data) = {len(result.data)}")
    
    print()
    
    # 测试4：兼容性 - 仍然可以解包
    print("测试 4: 向后兼容 - 仍可解包为两个变量")
    print("-" * 60)
    data, source = fetcher.fetch(
        stock_code='000001',
        stock_name='平安银行',
        start_date='20260210',
        end_date='20260210',
        adjust_type='qfq'
    )
    
    print(f"data 类型: {type(data)}")
    print(f"source: {source}")
    if data is not None:
        print(f"✅ 兼容旧的解包方式，数据源: {source}")

print()
print("=" * 60)
print("测试完成")
print("=" * 60)
