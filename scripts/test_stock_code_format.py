#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试股票代码前导零保留
验证所有数据源返回的股票代码都正确保留前导零
"""

from fetchers import AkshareFetcher, BaostockFetcher, YFinanceFetcher, MultiSourceFetcher

print("=" * 60)
print("测试股票代码前导零保留")
print("=" * 60)
print()

# 测试股票代码（带前导零）
test_codes = ['000001', '000002', '600519']

for stock_code in test_codes:
    print(f"测试股票代码: {stock_code}")
    print("-" * 60)
    
    # 测试 Baostock
    try:
        with BaostockFetcher() as bs_fetcher:
            df = bs_fetcher.fetch(stock_code, '20260210', '20260210')
            if df is not None and not df.empty:
                code_value = df['股票代码'].iloc[0]
                status = "✅" if code_value == stock_code else f"❌ (实际值: {code_value})"
                print(f"  Baostock:  {status}")
            else:
                print(f"  Baostock:  ⏭️  无数据")
    except Exception as e:
        print(f"  Baostock:  ❌ 错误: {str(e)[:30]}")
    
    # 测试 Akshare
    try:
        ak_fetcher = AkshareFetcher()
        df = ak_fetcher.fetch(stock_code, '20260210', '20260210', 'qfq')
        if df is not None and not df.empty:
            code_value = df['股票代码'].iloc[0]
            status = "✅" if code_value == stock_code else f"❌ (实际值: {code_value})"
            print(f"  Akshare:   {status}")
        else:
            print(f"  Akshare:   ⏭️  无数据")
    except Exception as e:
        print(f"  Akshare:   ❌ 错误: {str(e)[:30]}")
    
    # 测试 YFinance
    try:
        yf_fetcher = YFinanceFetcher()
        df = yf_fetcher.fetch(stock_code, '20260210', '20260210')
        if df is not None and not df.empty:
            code_value = df['股票代码'].iloc[0]
            status = "✅" if code_value == stock_code else f"❌ (实际值: {code_value})"
            print(f"  YFinance:  {status}")
        else:
            print(f"  YFinance:  ⏭️  无数据")
    except Exception as e:
        print(f"  YFinance:  ❌ 错误: {str(e)[:30]}")
    
    print()

print("=" * 60)
print("测试完成")
print("=" * 60)
print()
print("说明:")
print("  ✅ = 股票代码正确保留前导零")
print("  ❌ = 股票代码前导零丢失")
print("  ⏭️ = 无数据（可能是周末/节假日）")
