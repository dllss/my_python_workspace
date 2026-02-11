# -*- coding: utf-8 -*-
"""
数据源可用性检查脚本
检查 akshare、baostock、yfinance 三个数据源是否可用

使用方法：
    方式1（推荐）：
        make check-sources
    
    方式2：
        poetry run python check_data_sources.py
    
    方式3：
        python check_data_sources.py  # 需要先激活虚拟环境

说明：
    - 该脚本会逐个检查三个数据源的可用性
    - 检查过程可能需要几秒钟（需要网络请求）
    - 结果会显示每个数据源是否可用
    - 即使部分数据源不可用，系统仍能正常工作（会自动切换）
"""

from fetchers import MultiSourceFetcher


def main():
    print("=" * 60)
    print("数据源可用性检查")
    print("=" * 60)
    print()
    
    # 创建多数据源管理器
    fetcher = MultiSourceFetcher()
    
    # 检查所有数据源
    print("正在检查数据源...")
    status = fetcher.check_sources()
    
    print()
    print("检查结果:")
    print("-" * 60)
    
    # 打印结果
    sources = [
        ("baostock", "Baostock（优先）"),
        ("akshare", "Akshare"),
        ("yfinance", "YFinance（备用）")
    ]
    
    available_count = 0
    for source_key, source_name in sources:
        is_available = status.get(source_key, False)
        status_icon = "✅" if is_available else "❌"
        status_text = "可用" if is_available else "不可用"
        print(f"{status_icon} {source_name}: {status_text}")
        if is_available:
            available_count += 1
    
    print()
    print("=" * 60)
    print(f"总结: {available_count}/3 个数据源可用")
    
    if available_count == 0:
        print("⚠️  警告: 所有数据源都不可用，请检查网络连接和依赖包安装")
    elif available_count < 3:
        print("⚠️  注意: 部分数据源不可用，但仍可正常获取数据")
    else:
        print("✅ 所有数据源正常")
    
    print("=" * 60)
    
    # 清理资源
    fetcher.close()


if __name__ == "__main__":
    main()
