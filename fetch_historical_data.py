# -*- coding: utf-8 -*-
"""
批量获取A股股票历史数据脚本 V2（推荐）
多数据源混合策略 + 增强错误处理 + 智能增量更新
支持：baostock（优先）、akshare、yfinance 三个数据源自动切换

使用方法：
    方式1（推荐）：
        make run-all-v2
    
    方式2：
        poetry run python fetch_all_stock_hist_v2.py
    
    方式3：
        python fetch_all_stock_hist_v2.py  # 需要先激活虚拟环境

主要功能：
    ✅ 多数据源自动切换（提高稳定性）
    ✅ 智能增量更新（只获取缺失的数据）
    ✅ 交易日识别（自动跳过节假日/周末）
    ✅ 停牌智能处理（避免无效请求）
    ✅ 批量处理（支持分批获取）
    ✅ 失败重试（数据源异常自动切换）
    ✅ 双重数据完整性保护（时间检查 + 标记机制）

配置参数（在 config.py 中修改）：
    - START_DATE: 开始日期（默认 "20000101"）
    - END_DATE: 结束日期（默认昨天）
    - ADJUST_TYPE: 复权类型（默认 "qfq" 前复权）
    - BATCH_SIZE: 批次大小（0=全部，>0=分批）
    - START_INDEX: 起始索引（分批处理时使用）
    - UPDATE_MODE: 更新模式（见下方说明）
    - DELAY_MIN/MAX: 请求延迟（避免频繁请求）

更新模式说明：
    - tail: 只补充尾部数据，忽略中间缺失（默认，推荐）
      适用：日常增量更新，避免对停牌日反复请求
    - full: 完全刷新，补充所有缺失数据
      适用：初次获取或数据修复（会对停牌日发起请求）
    - head_tail: 补充头尾，忽略中间缺失
      适用：扩展历史数据范围

数据源优先级：
    1. Baostock（优先，数据质量高）
    2. Akshare（备用，覆盖全面）
    3. YFinance（备用，国际接口）

输出说明：
    - 数据文件：data/CN/stock_{代码}.csv
    - 失败列表：data/CN/failed_stocks.csv
    - 统计信息：新增/更新/跳过/失败数量
    - 数据源使用统计

数据完整性保护：
    🛡️ 第一层：18:30 之前自动使用昨天（BaoStock 18:00 后才完整更新）
    🛡️ 第二层：不完整数据标记机制（异常情况下的备用保险）
    💡 推荐：在 18:30 之后运行脚本以获取完整的当日数据

注意事项：
    ⚠️ 中间缺失通常是个股停牌导致，非数据丢失
    ⚠️ 首次运行前需先获取股票列表：make run-list
    ⚠️ 批量获取需要较长时间，建议使用分批模式
    ⚠️ 数据源可能限流，脚本已加入随机延迟
"""

import os
import time
import random
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import pandas as pd
from config import (
    OUTPUT_DIR,
    CN_DIR,
    START_DATE,
    END_DATE,
    ADJUST_TYPE,
    DELAY_MIN,
    DELAY_MAX,
    BATCH_SIZE,
    START_INDEX,
    UPDATE_MODE,
    PREFERRED_SOURCE,
)
from fetchers import MultiSourceFetcher
from utils import (
    has_trading_day,
    get_missing_date_range,
    get_safe_end_date,
    MetadataManager,
    save_dataframe,
    merge_and_save_data
)

# ========== 日志配置 ==========
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG: 显示详细调试信息 | INFO: 只显示关键信息（推荐）
    format='%(message)s'  # 简化格式，只显示消息
)
logger = logging.getLogger(__name__)

# ========== 初始化 ==========
cn_dir = os.path.join(OUTPUT_DIR, CN_DIR)
stock_list_file = os.path.join(cn_dir, "stock_list.csv")

# ========== 检查并调整结束日期（18:30之前强制使用昨天） ==========
SAFE_END_DATE, date_adjusted = get_safe_end_date(END_DATE)

# ========== 市场状态检查已由 get_safe_end_date 完成 ==========
# 18:30 之前已自动调整为昨天，无需额外检查

# ========== 读取股票列表 ==========
logger.info(f"正在读取股票列表: {stock_list_file}")
logger.info(f"目标日期范围: {START_DATE} ~ {SAFE_END_DATE}")

try:
    stock_list = pd.read_csv(stock_list_file, dtype={'code': str})
    stock_list['code'] = stock_list['code'].str.zfill(6)
    total_stock_length = len(stock_list)
except FileNotFoundError:
    logger.error(f"错误: 股票列表文件不存在: {stock_list_file}")
    logger.error("请先运行 'make run-list' 或 'poetry run python fetch_stock_list.py' 获取股票列表")
    exit(1)
except Exception as e:
    logger.error(f"读取股票列表失败: {e}")
    exit(1)

# ========== 分批处理 ==========
if BATCH_SIZE > 0:
    end_index = min(START_INDEX + BATCH_SIZE, total_stock_length)
    stock_list = stock_list[START_INDEX:end_index]
    logger.info(f"分批模式: 处理第 {START_INDEX + 1} 到第 {end_index} 只股票 (共 {len(stock_list)} 只)")
    logger.info(f"总进度: {end_index}/{total_stock_length} ({end_index/total_stock_length*100:.1f}%)\n")
else:
    logger.info(f"全量模式: 共 {total_stock_length} 只股票\n")

# ========== 批量获取数据 ==========
success_count = 0
fail_count = 0
skip_count = 0
update_count = 0
failed_stocks = []
fetch_times: List[float] = []  # 记录每只股票的获取耗时

# 记录开始时间
start_time = time.time()

# 初始化元数据管理器
metadata_mgr = MetadataManager(cn_dir)
logger.debug(f"元数据管理器初始化: {metadata_mgr.get_stats()}")

# 使用多数据源管理器
logger.info(f"优先数据源: {PREFERRED_SOURCE}")
with MultiSourceFetcher(preferred_source=PREFERRED_SOURCE) as multi_fetcher:
    for idx, (index, row) in enumerate(stock_list.iterrows(), 1):
        stock_code = row['code']
        stock_name = row['name']
        output_file = os.path.join(cn_dir, f"stock_{stock_code}.csv")
        
        # 记录单只股票开始时间
        stock_start_time = time.time()
        
        if BATCH_SIZE > 0:
            print(f"[{idx}/{len(stock_list)}] {stock_code} {stock_name} ", end="")
        else:
            print(f"[{index + 1}/{total_stock_length}] {stock_code} {stock_name} ", end="")
        
        try:
            # 检查是否需要更新（使用元数据加速）
            need_update, fetch_start, fetch_end, need_full_refresh, missing_dates = get_missing_date_range(
                existing_file=output_file,
                start_date=START_DATE,
                end_date=SAFE_END_DATE,
                update_mode=UPDATE_MODE,
                metadata_manager=metadata_mgr
            )
            
            if not need_update:
                stock_elapsed = time.time() - stock_start_time
                print(f"⏭️  已是最新 ({stock_elapsed:.2f}s)")
                skip_count += 1
                # 不需要更新元数据，因为元数据已经在之前的运行中正确设置
                # 如果这里重新读取CSV更新元数据，会导致停牌期间的日期被错误覆盖
                continue
            
            # 检查是否有交易日（周末或节假日跳过）
            if not has_trading_day(start_date=fetch_start, end_date=fetch_end):
                stock_elapsed = time.time() - stock_start_time
                print(f"获取 {fetch_start}~{fetch_end}... ⏭️  非交易日，跳过 ({stock_elapsed:.2f}s)")
                skip_count += 1
                continue
            
            # 多数据源获取
            if need_full_refresh:
                print(f"检测到中间缺失，重新获取 {fetch_start}~{fetch_end}...", end=" ")
                # 打印缺失的日期列表
                if missing_dates:
                    print()  # 换行
                    print(f"\t缺失日期: {', '.join(missing_dates)}")
                    print(f"\t正在获取...", end=" ")
            else:
                print(f"获取 {fetch_start}~{fetch_end}...", end=" ")
            
            result = multi_fetcher.fetch(
                stock_code=stock_code,
                stock_name=stock_name,
                start_date=fetch_start,
                end_date=fetch_end,
                adjust_type=ADJUST_TYPE
            )
            
            if result.data is None:
                stock_elapsed = time.time() - stock_start_time
                if result.source == "no_data":
                    # 数据源正常但无数据（节假日/停牌/未上市等）
                    print(f"⏭️  无数据（节假日/停牌） ({stock_elapsed:.2f}s)")
                    skip_count += 1
                    # 更新元数据，避免重复拉取
                    metadata_mgr.update_last_date(stock_code, fetch_end)
                else:
                    # 有数据源报错，真正的失败
                    print(f"❌ 所有数据源均失败 ({stock_elapsed:.2f}s)")
                    fail_count += 1
                    failed_stocks.append({"code": stock_code, "name": stock_name, "reason": "所有数据源均失败"})
                    # 不更新元数据，下次需要重试
                continue
            
            # 获取数据和数据源
            df_new = result.data
            source = result.source
            
            # 合并并保存数据（同时更新元数据）
            # 保留历史名称策略：不修改历史数据，新数据使用最新名称，可记录名称变化历史
            # 传递 fetch_end 作为元数据更新的日期，避免因停牌导致重复拉取
            is_update, new_count, removed_count = merge_and_save_data(
                df_new, output_file, stock_code, need_full_refresh, metadata_mgr, fetch_end
            )
            
            # 计算耗时
            stock_elapsed = time.time() - stock_start_time
            fetch_times.append(stock_elapsed)
            
            # 构建输出信息
            status = '✅ ' + ('刷新' if need_full_refresh else '更新' if is_update else '新增')
            data_info = f"(+{new_count} 条"
            if removed_count > 0:
                data_info += f", 过滤{removed_count}条停牌"
            data_info += f") [{source}] ({stock_elapsed:.2f}s)"
            
            print(f"{status} {data_info}")
            
            if is_update:
                update_count += 1
            else:
                success_count += 1
            
            # 随机延迟
            delay = random.uniform(DELAY_MIN, DELAY_MAX)
            logger.info(f"⏸️  延迟 {delay:.2f}s 后继续...")
            time.sleep(delay)
            
        except KeyboardInterrupt:
            logger.warning("\n\n⚠️  用户中断，正在保存已处理的数据...")
            break
        except FileNotFoundError as e:
            logger.error(f"❌ 文件错误: {str(e)}")
            fail_count += 1
            failed_stocks.append({"code": stock_code, "name": stock_name, "reason": f"文件错误: {str(e)}"})
        except pd.errors.EmptyDataError as e:
            logger.error(f"❌ 数据为空: {str(e)}")
            fail_count += 1
            failed_stocks.append({"code": stock_code, "name": stock_name, "reason": f"数据为空: {str(e)}"})
        except Exception as e:
            logger.error(f"❌ 异常: {str(e)}")
            fail_count += 1
            failed_stocks.append({"code": stock_code, "name": stock_name, "reason": str(e)})
    
    # 获取数据源使用统计
    source_stats = multi_fetcher.get_stats()

# 计算总耗时
total_elapsed = time.time() - start_time

# ========== 统计结果 ==========
logger.info("\n" + "=" * 60)
logger.info("批量获取完成")
logger.info("=" * 60)
if BATCH_SIZE > 0:
    logger.info(f"本批处理: {len(stock_list)} 只股票")
    logger.info(f"总进度: {end_index}/{total_stock_length} ({end_index/total_stock_length*100:.1f}%)")
else:
    logger.info(f"总计: {total_stock_length} 只股票")
logger.info(f"新增: {success_count} 只")
logger.info(f"更新: {update_count} 只")
logger.info(f"跳过: {skip_count} 只（数据已是最新）")
logger.info(f"失败: {fail_count} 只")

logger.info(f"\n数据源使用统计:")
logger.info(f"  akshare: {source_stats['akshare']} 只")
logger.info(f"  baostock: {source_stats['baostock']} 只")
logger.info(f"  yfinance: {source_stats['yfinance']} 只")

# 耗时统计
logger.info(f"\n⏱️  耗时统计:")
logger.info(f"  总耗时: {total_elapsed:.2f}s ({total_elapsed/60:.2f}min)")
if fetch_times:
    avg_time = sum(fetch_times) / len(fetch_times)
    max_time = max(fetch_times)
    min_time = min(fetch_times)
    logger.info(f"  平均耗时: {avg_time:.2f}s/只")
    logger.info(f"  最快: {min_time:.2f}s")
    logger.info(f"  最慢: {max_time:.2f}s")
    logger.info(f"  实际获取: {len(fetch_times)} 只")
if len(stock_list) > 0:
    logger.info(f"  平均速度: {len(stock_list)/total_elapsed:.2f} 只/秒")

if BATCH_SIZE > 0 and end_index < total_stock_length:
    next_start = end_index
    logger.info(f"\n💡 提示: 还有 {total_stock_length - end_index} 只股票未处理")

if failed_stocks:
    logger.info(f"\n失败列表:")
    for stock in failed_stocks:
        logger.info(f"  - {stock['code']} {stock['name']}: {stock['reason']}")
    
    # 保存失败列表
    try:
        failed_df = pd.DataFrame(failed_stocks)
        failed_file = os.path.join(cn_dir, "failed_stocks.csv")
        failed_df.to_csv(failed_file, index=False, encoding="utf-8-sig")
        logger.info(f"\n失败列表已保存到: {failed_file}")
    except Exception as e:
        logger.error(f"保存失败列表时出错: {e}")

# ========== 脚本执行完成 ==========
# 18:30 时间检查已确保数据完整性，无需额外提醒
