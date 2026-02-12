# -*- coding: utf-8 -*-
"""
A股股票列表获取脚本
获取全部 A 股股票代码和名称，并导出到 CSV 文件，同时对比变化

使用方法：
    方式1（推荐）：
        make list
    
    方式2：
        poetry run python fetch_stock_list.py
    
    方式3：
        python fetch_stock_list.py  # 需要先激活虚拟环境

功能说明：
    - 使用 akshare 的 stock_info_a_code_name() 接口
    - 获取全部 A 股股票列表（沪深两市所有板块）
    - 包含股票代码和股票名称
    - 自动导出为 CSV 文件：data/CN/stock_list.csv
    - 对比旧文件，显示新增/删除的股票
    - 为新增股票生成历史数据获取命令
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
import pandas as pd
import akshare as ak
from datetime import datetime
from config import OUTPUT_DIR, CN_DIR, STOCK_LIST_FILE

# ========== 日志配置 ==========
# 创建日志目录
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# 使用固定的日志文件名（按日期），追加模式
log_filename = os.path.join(log_dir, f"stock_list_{datetime.now().strftime('%Y%m%d')}.log")

# 打开日志文件（追加模式）
log_file = open(log_filename, 'a', encoding='utf-8')

def log_both(msg):
    """同时输出到控制台和文件"""
    print(msg)
    log_file.write(msg + '\n')
    log_file.flush()

def log_console(msg):
    """仅输出到控制台"""
    print(msg)

def log_file_only(msg):
    """仅输出到文件"""
    log_file.write(msg + '\n')
    log_file.flush()


def load_old_stock_list(file_path: str) -> pd.DataFrame:
    """
    加载旧的股票列表
    
    Args:
        file_path: 股票列表文件路径
    
    Returns:
        DataFrame: 旧的股票列表，如果文件不存在则返回空 DataFrame
    """
    if os.path.exists(file_path):
        try:
            return pd.read_csv(file_path, dtype={'code': str})
        except Exception as e:
            log_console(f"⚠️  加载旧股票列表失败: {e}")
            return pd.DataFrame(columns=['code', 'name'])
    return pd.DataFrame(columns=['code', 'name'])


def compare_stock_lists(old_df: pd.DataFrame, new_df: pd.DataFrame) -> tuple:
    """
    对比新旧股票列表，找出新增和删除的股票
    
    Args:
        old_df: 旧股票列表
        new_df: 新股票列表
    
    Returns:
        tuple: (新增股票DataFrame, 删除股票DataFrame)
    """
    if old_df.empty:
        return new_df, pd.DataFrame(columns=['code', 'name'])
    
    # 提取股票代码集合
    old_codes = set(old_df['code'])
    new_codes = set(new_df['code'])
    
    # 找出新增和删除的股票代码
    added_codes = new_codes - old_codes
    removed_codes = old_codes - new_codes
    
    # 提取对应的股票信息
    added_stocks = new_df[new_df['code'].isin(added_codes)].sort_values('code')
    removed_stocks = old_df[old_df['code'].isin(removed_codes)].sort_values('code')
    
    return added_stocks, removed_stocks


# ========== 主程序 ==========
log_both("="*80)
log_both("A股股票列表获取脚本")
log_both("="*80)
log_both(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log_both(f"日志文件: {log_filename}")
log_console("")

# 1. 确定输出路径
cn_output_dir = os.path.join(OUTPUT_DIR, CN_DIR)
os.makedirs(cn_output_dir, exist_ok=True)
output_file = os.path.join(cn_output_dir, STOCK_LIST_FILE)

# 2. 加载旧的股票列表（如果存在）
log_console("检查旧股票列表...")
old_stock_list = load_old_stock_list(output_file)

if old_stock_list.empty:
    log_console("✅ 首次运行，将获取全部股票列表")
else:
    log_console(f"✅ 找到旧股票列表，共 {len(old_stock_list)} 只股票")

log_console("")

# 3. 获取新的股票列表
log_console("正在从 AkShare 获取最新股票列表...")
try:
    new_stock_list = ak.stock_info_a_code_name()
    log_console(f"✅ 获取成功，共 {len(new_stock_list)} 只股票")
except Exception as e:
    log_both(f"❌ 获取失败: {e}")
    log_both("")
    log_both("="*80)
    log_both("执行完成")
    log_both("="*80)
    log_file_only("")
    log_file.close()
    exit(1)

log_console("")

# 4. 对比新旧列表并输出统计信息
if not old_stock_list.empty:
    log_console("="*80)
    log_console("对比新旧股票列表")
    log_console("="*80)
    log_console("")
    
    added_stocks, removed_stocks = compare_stock_lists(old_stock_list, new_stock_list)
    
    # 显示统计信息（文件+控制台）
    log_both(f"旧列表股票数: {len(old_stock_list)}")
    log_both(f"新列表股票数: {len(new_stock_list)}")
    log_both(f"新增股票数: {len(added_stocks)}")
    log_both(f"删除股票数: {len(removed_stocks)}")
    log_both("")
    
    # 显示新增股票
    if not added_stocks.empty:
        log_both("="*80)
        log_both(f"🆕 新增股票列表（共 {len(added_stocks)} 只）")
        log_both("="*80)
        log_both("")
        
        # 仅控制台：显示"按股票代码排序:"
        log_console("按股票代码排序:")
        
        for _, row in added_stocks.iterrows():
            code = row['code']
            name = row['name']
            
            # 判断板块
            if code.startswith('000') or code.startswith('001'):
                market = '深圳主板'
            elif code.startswith('002'):
                market = '深圳中小板'
            elif code.startswith('003'):
                market = '深圳'
            elif code.startswith('300'):
                market = '创业板'
            elif code.startswith('600') or code.startswith('601') or code.startswith('603'):
                market = '上海主板'
            elif code.startswith('688'):
                market = '科创板'
            else:
                market = '其他'
            
            log_both(f"  {code} {name:12s} [{market}]")
        
        log_both("")
        
        # 仅控制台：显示建议操作
        log_console("-"*80)
        log_console("💡 建议操作：为这些新股获取历史数据")
        log_console("-"*80)
        log_console("")
        log_console("方式1: 逐个获取（推荐，可以指定日期范围）")
        log_console("")
        
        for _, row in added_stocks.head(10).iterrows():
            code = row['code']
            name = row['name']
            log_console(f"  make single CODE={code}  # {name}")
        
        if len(added_stocks) > 10:
            log_console(f"  ... 还有 {len(added_stocks) - 10} 只股票")
        
        log_console("")
        log_console("方式2: 批量获取（需要修改 fetch_historical_data.py 支持股票代码过滤）")
        log_console(f"  # 将以下代码保存为文件，然后运行")
        log_console(f"  codes = {','.join(added_stocks['code'].tolist())}")
        log_console("")
        log_console("方式3: 手动运行 make history（会自动跳过已有数据的股票）")
        log_console("  make history")
        log_console("")
    else:
        log_console("✅ 没有新增股票")
        log_console("")
    
    # 显示删除股票
    if not removed_stocks.empty:
        log_both("="*80)
        log_both(f"🗑️  删除/退市股票列表（共 {len(removed_stocks)} 只）")
        log_both("="*80)
        log_both("")
        
        for _, row in removed_stocks.iterrows():
            code = row['code']
            name = row['name']
            log_both(f"  {code} {name}")
        
        log_both("")
        
        # 仅控制台：显示警告信息
        log_console("⚠️  这些股票已从交易所退市或停止交易")
        log_console("   data/CN/ 目录中的历史数据文件将保留")
        log_console("")
    else:
        log_console("✅ 没有删除/退市的股票")
        log_console("")

# 5. 保存新的股票列表
log_both("="*80)
log_both("保存股票列表")
log_both("="*80)
log_both("")

new_stock_list.to_csv(output_file, index=False, encoding="utf-8-sig")
log_both(f"✅ 数据已保存到: {output_file}")
log_both(f"   文件大小: {os.path.getsize(output_file) / 1024:.2f} KB")
log_both("")

log_both("="*80)
log_both("执行完成")
log_both("="*80)

# 文件日志结尾添加空行
log_file_only("")

# 关闭日志文件
log_file.close()
