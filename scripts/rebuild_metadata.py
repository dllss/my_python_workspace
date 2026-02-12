# -*- coding: utf-8 -*-
"""
重建元数据文件

从现有的股票 CSV 文件重建元数据，提高后续检查性能

使用方法：
    make rebuild-metadata
    或
    poetry run python rebuild_metadata.py
"""

import os
import pandas as pd
from config import OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE
from utils import MetadataManager


def main():
    cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
    stock_list_file = os.path.join(cn_dir, STOCK_LIST_FILE)
    
    print("=" * 60)
    print("重建元数据文件")
    print("=" * 60)
    
    # 检查股票列表是否存在
    if not os.path.exists(stock_list_file):
        print(f"❌ 错误: 股票列表文件不存在: {stock_list_file}")
        print("请先运行 'make run-list' 获取股票列表")
        return
    
    # 读取股票列表
    print(f"正在读取股票列表: {stock_list_file}")
    stock_list = pd.read_csv(stock_list_file, dtype={'code': str})
    stock_list['code'] = stock_list['code'].str.zfill(6)
    stock_codes = stock_list['code'].tolist()
    print(f"共 {len(stock_codes)} 只股票\n")
    
    # 初始化元数据管理器
    metadata_mgr = MetadataManager(cn_dir)
    
    # 重建元数据
    print("正在扫描 CSV 文件...")
    success_count = metadata_mgr.rebuild_from_files(stock_codes)
    
    # 显示统计
    stats = metadata_mgr.get_stats()
    print("\n" + "=" * 60)
    print("重建完成")
    print("=" * 60)
    print(f"成功: {success_count}/{len(stock_codes)} 只股票")
    print(f"元数据文件: {metadata_mgr.metadata_file}")
    print(f"\n💡 提示: 元数据文件会在每次数据更新时自动维护")


if __name__ == "__main__":
    main()
