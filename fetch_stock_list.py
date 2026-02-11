# -*- coding: utf-8 -*-
"""
A股股票列表获取脚本
获取全部 A 股股票代码和名称，并导出到 CSV 文件

使用方法：
    方式1（推荐）：
        make run-list
    
    方式2：
        poetry run python fetch_stock_list.py
    
    方式3：
        python fetch_stock_list.py  # 需要先激活虚拟环境

功能说明：
    - 使用 akshare 的 stock_info_a_code_name() 接口
    - 获取全部 A 股股票列表（沪深两市所有板块）
    - 包含股票代码和股票名称
    - 自动导出为 CSV 文件：data/CN/stock_list.csv
    - 后续批量获取脚本会读取这个列表

包含的板块：
    ✅ 深圳主板   (000xxx)：约 414 只
    ✅ 深圳中小板 (002xxx)：约 922 只
    ✅ 创业板     (300xxx)：约 939 只
    ✅ 上海主板   (6xxxxx)：约 1703 只
    ✅ 科创板     (688xxx)：约 603 只
    ❌ 北交所     (8xxxxx)：不包含
    
    总计约 5483 只股票（数据会随新股上市/退市而变化）

输出文件格式：
    code,name
    000001,平安银行
    000002,万科A
    300001,特锐德      # 创业板
    688001,华兴源创    # 科创板
    ...

注意事项：
    - 首次运行或股票列表有更新时需要重新获取
    - 每年可能有新股上市或退市，建议定期更新（如每月一次）
    - 该脚本仅获取列表，不获取历史数据
    - 获取过程需要约 10-20 秒（akshare 会遍历多个交易所）
"""

import os
import akshare as ak
from config import OUTPUT_DIR, CN_DIR

# ========== 获取股票列表 ==========
print("正在获取 A 股全部股票列表...")

stock_list = ak.stock_info_a_code_name()

print(f"获取成功，共 {len(stock_list)} 只股票\n")

# # ========== 数据展示 ==========
# print("=" * 50)
# print("股票列表预览（前10条）:")
# print("=" * 50)
# print(stock_list.head(10))

# ========== 导出数据 ==========
cn_output_dir = os.path.join(OUTPUT_DIR, CN_DIR)
os.makedirs(cn_output_dir, exist_ok=True)

output_file = os.path.join(cn_output_dir, "stock_list.csv")
stock_list.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"\n数据已导出到: {output_file}")
