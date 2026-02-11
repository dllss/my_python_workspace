#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试三个数据源返回的数据格式是否一致
"""

from fetchers import AkshareFetcher, BaostockFetcher, YFinanceFetcher

def test_data_format():
    """测试数据格式一致性"""
    stock_code = '000001'
    start_date = '20260206'
    end_date = '20260210'
    
    print("=" * 80)
    print("测试三个数据源的数据格式一致性")
    print("=" * 80)
    
    # 1. 测试 AkshareFetcher
    print("\n【1】测试 AkshareFetcher")
    print("-" * 80)
    akshare_fetcher = AkshareFetcher()
    df_akshare = akshare_fetcher.fetch(stock_code, start_date, end_date, 'qfq')
    
    if df_akshare is not None:
        print(f"✅ 成功获取 {len(df_akshare)} 条数据")
        print(f"列名: {list(df_akshare.columns)}")
        print(f"数据类型:\n{df_akshare.dtypes}")
        print(f"\n前3行数据:\n{df_akshare.head(3)}")
    else:
        print("❌ 获取失败")
    
    # 2. 测试 BaostockFetcher
    print("\n【2】测试 BaostockFetcher")
    print("-" * 80)
    with BaostockFetcher() as baostock_fetcher:
        df_baostock = baostock_fetcher.fetch(stock_code, start_date, end_date)
        
        if df_baostock is not None:
            print(f"✅ 成功获取 {len(df_baostock)} 条数据")
            print(f"列名: {list(df_baostock.columns)}")
            print(f"数据类型:\n{df_baostock.dtypes}")
            print(f"\n前3行数据:\n{df_baostock.head(3)}")
        else:
            print("❌ 获取失败")
    
    # 3. 测试 YFinanceFetcher
    print("\n【3】测试 YFinanceFetcher")
    print("-" * 80)
    yfinance_fetcher = YFinanceFetcher()
    df_yfinance = yfinance_fetcher.fetch(stock_code, start_date, end_date)
    
    if df_yfinance is not None:
        print(f"✅ 成功获取 {len(df_yfinance)} 条数据")
        print(f"列名: {list(df_yfinance.columns)}")
        print(f"数据类型:\n{df_yfinance.dtypes}")
        print(f"\n前3行数据:\n{df_yfinance.head(3)}")
    else:
        print("❌ 获取失败")
    
    # 4. 对比分析
    print("\n" + "=" * 80)
    print("【4】格式一致性分析")
    print("=" * 80)
    
    # 收集所有成功的数据源
    results = []
    if df_akshare is not None:
        results.append(("akshare", df_akshare))
    if df_baostock is not None:
        results.append(("baostock", df_baostock))
    if df_yfinance is not None:
        results.append(("yfinance", df_yfinance))
    
    if len(results) < 2:
        print("⚠️  至少需要2个数据源成功才能对比")
        return
    
    # 对比列名
    print("\n【列名对比】")
    base_name, base_df = results[0]
    base_columns = set(base_df.columns)
    
    all_consistent = True
    for name, df in results[1:]:
        current_columns = set(df.columns)
        if current_columns == base_columns:
            print(f"✅ {name} 与 {base_name} 列名一致")
        else:
            print(f"❌ {name} 与 {base_name} 列名不一致")
            print(f"   {base_name} 独有: {base_columns - current_columns}")
            print(f"   {name} 独有: {current_columns - base_columns}")
            all_consistent = False
    
    # 对比列顺序
    print("\n【列顺序对比】")
    base_column_list = list(base_df.columns)
    for name, df in results[1:]:
        current_column_list = list(df.columns)
        if current_column_list == base_column_list:
            print(f"✅ {name} 与 {base_name} 列顺序一致")
        else:
            print(f"❌ {name} 与 {base_name} 列顺序不一致")
            print(f"   {base_name}: {base_column_list}")
            print(f"   {name}: {current_column_list}")
            all_consistent = False
    
    # 对比数据类型
    print("\n【数据类型对比】")
    for name, df in results[1:]:
        type_match = True
        for col in base_columns & set(df.columns):
            if base_df[col].dtype != df[col].dtype:
                if not type_match:
                    print(f"❌ {name} 与 {base_name} 数据类型不一致")
                    type_match = False
                print(f"   列 '{col}': {base_name}={base_df[col].dtype}, {name}={df[col].dtype}")
                all_consistent = False
        if type_match:
            print(f"✅ {name} 与 {base_name} 数据类型一致")
    
    # 对比数值精度
    print("\n【数值精度对比】")
    numeric_cols = ['开盘', '收盘', '最高', '最低', '涨跌幅', '涨跌额', '振幅', '换手率']
    for col in numeric_cols:
        if col in base_df.columns:
            for name, df in results[1:]:
                if col in df.columns:
                    # 检查小数位数
                    base_sample = base_df[col].iloc[1] if len(base_df) > 1 else base_df[col].iloc[0]
                    current_sample = df[col].iloc[1] if len(df) > 1 else df[col].iloc[0]
                    
                    base_decimals = len(str(base_sample).split('.')[-1]) if '.' in str(base_sample) else 0
                    current_decimals = len(str(current_sample).split('.')[-1]) if '.' in str(current_sample) else 0
                    
                    if base_decimals == current_decimals:
                        print(f"✅ 列 '{col}': 精度一致 ({base_decimals} 位小数)")
                    else:
                        print(f"❌ 列 '{col}': 精度不一致 ({base_name}={base_decimals}, {name}={current_decimals})")
                        all_consistent = False
    
    # 最终结论
    print("\n" + "=" * 80)
    if all_consistent:
        print("🎉 结论: 所有数据源的格式完全一致！")
    else:
        print("⚠️  结论: 数据源格式存在差异，请检查上述不一致项")
    print("=" * 80)


if __name__ == "__main__":
    test_data_format()
