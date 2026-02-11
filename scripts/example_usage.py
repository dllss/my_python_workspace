#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
示例：如何使用 fetchers 模块获取股票数据
"""

from fetchers import AkshareFetcher, BaostockFetcher, YFinanceFetcher, MultiSourceFetcher

print("=" * 60)
print("示例 1: 使用 AkshareFetcher")
print("=" * 60)
akshare_fetcher = AkshareFetcher()
df = akshare_fetcher.fetch('000001', '20260206', '20260210', 'qfq')
if df is not None:
    print(f"✅ 成功获取 {len(df)} 条数据")
    print(df.head(3))
else:
    print("❌ 获取数据失败")

print("\n" + "=" * 60)
print("示例 2: 使用 BaostockFetcher（推荐使用上下文管理器）")
print("=" * 60)
with BaostockFetcher() as baostock_fetcher:
    df = baostock_fetcher.fetch('000002', '20260206', '20260210')
    if df is not None:
        print(f"✅ 成功获取 {len(df)} 条数据")
        print(df.head(3))
    else:
        print("❌ 获取数据失败")

print("\n" + "=" * 60)
print("示例 3: 使用 YFinanceFetcher")
print("=" * 60)
yfinance_fetcher = YFinanceFetcher()
df = yfinance_fetcher.fetch('000001', '20260206', '20260210')
if df is not None:
    print(f"✅ 成功获取 {len(df)} 条数据")
    print(df.head(3))
else:
    print("❌ 获取数据失败")

print("\n" + "=" * 60)
print("示例 4: 使用 MultiSourceFetcher（推荐）")
print("=" * 60)
print("自动按优先级尝试：akshare -> baostock -> yfinance")

with MultiSourceFetcher() as multi_fetcher:
    # 获取单只股票数据
    result = multi_fetcher.fetch('000001', '平安银行', '20260206', '20260210', 'qfq')
    
    if result.data is not None:
        print(f"\n✅ 成功！使用数据源: {result.source}")
        print(f"获取 {len(result.data)} 条数据")
        print(result.data.head(3))
        
        # 查看统计信息
        print(f"\n数据源使用统计: {multi_fetcher.get_stats()}")
    elif result.source == "no_data":
        print("⏭️  无数据（节假日/停牌等）")
    else:
        print("❌ 所有数据源均失败")

print("\n" + "=" * 60)
print("示例 5: 手动降级策略（不推荐，仅供参考）")
print("=" * 60)
stock_code = '000002'
start_date = '20260206'
end_date = '20260210'

# 按优先级尝试不同数据源
fetchers_list = [
    ("akshare", AkshareFetcher()),
    ("baostock", BaostockFetcher()),
    ("yfinance", YFinanceFetcher())
]

for source_name, fetcher in fetchers_list:
    print(f"\n尝试 {source_name}...", end=" ")
    try:
        if source_name == "baostock":
            fetcher.login()
        
        if source_name == "akshare":
            df = fetcher.fetch(stock_code, start_date, end_date, 'qfq')
        else:
            df = fetcher.fetch(stock_code, start_date, end_date)
        
        if df is not None and not df.empty:
            print(f"✅ 成功！获取 {len(df)} 条数据")
            print(df.head(3))
            break
        else:
            print("❌ 无数据")
    except Exception as e:
        print(f"❌ 失败: {str(e)[:50]}")
    finally:
        if source_name == "baostock" and hasattr(fetcher, 'is_logged_in') and fetcher.is_logged_in:
            fetcher.logout()
