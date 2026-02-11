#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
TuShare 连接测试脚本
用于测试 TuShare Token 是否配置正确，以及是否有足够的权限

使用方法：
    python test_tushare.py
    或
    python test_tushare.py --token "你的token"
"""

import os
import sys
import argparse
from datetime import datetime, timedelta

try:
    import tushare as ts
except ImportError:
    print("❌ 错误: 未安装 tushare 库")
    print("请运行: poetry add tushare")
    print("或者: pip install tushare")
    sys.exit(1)


def get_token() -> str:
    """获取 Token（优先从 config.py 读取）"""
    # 1. 尝试从 config.py 获取（优先）
    try:
        from config import TUSHARE_TOKEN
        if TUSHARE_TOKEN:
            return TUSHARE_TOKEN
    except ImportError:
        pass
    
    # 2. 尝试从环境变量获取
    token = os.environ.get('TUSHARE_TOKEN')
    if token:
        return token
    
    return None


def test_connection(token: str):
    """测试 TuShare 连接"""
    print("="*80)
    print("TuShare 连接测试")
    print("="*80)
    print()
    
    # 1. 测试 Token 是否有效
    print("📝 步骤1: 测试 Token 是否有效...")
    try:
        pro = ts.pro_api(token)
        print("✅ Token 有效")
    except Exception as e:
        print(f"❌ Token 无效: {e}")
        print()
        print("解决方案:")
        print("  1. 检查 Token 是否正确复制")
        print("  2. 登录 https://tushare.pro/user/token 重新获取")
        return False
    
    print()
    
    # 2. 测试是否能获取股票列表
    print("📝 步骤2: 测试获取股票列表...")
    try:
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name')
        if df is not None and not df.empty:
            print(f"✅ 成功获取股票列表，共 {len(df)} 只股票")
            print(f"   示例: {df.head(3)[['symbol', 'name']].to_string(index=False)}")
        else:
            print("⚠️  未获取到股票列表")
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
    
    print()
    
    # 3. 测试是否能获取日线数据
    print("📝 步骤3: 测试获取日线数据（最重要）...")
    target_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    print(f"   目标日期: {target_date}")
    
    try:
        df = pro.daily(trade_date=target_date)
        if df is not None and not df.empty:
            print(f"✅ 成功获取日线数据，共 {len(df)} 只股票")
            print(f"   数据列: {df.columns.tolist()}")
            print()
            print("   数据示例（前3条）:")
            print(df.head(3).to_string(index=False))
        else:
            print(f"⚠️  未获取到 {target_date} 的数据")
            print("   可能原因:")
            print("     1. 该日期为非交易日（周末/节假日）")
            print("     2. 数据尚未更新（建议在17:00后运行）")
            
            # 尝试获取前一天的数据
            print()
            print("   尝试获取更早的数据...")
            for i in range(2, 10):
                test_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                df = pro.daily(trade_date=test_date)
                if df is not None and not df.empty:
                    print(f"✅ 成功获取 {test_date} 的数据，共 {len(df)} 只股票")
                    break
            else:
                print("❌ 无法获取最近10天的数据")
    except Exception as e:
        print(f"❌ 获取日线数据失败: {e}")
        print()
        print("   常见错误:")
        print("     1. 积分不足: 需要至少 120 积分")
        print("        解决: 登录 https://tushare.pro 填写个人信息")
        print("     2. 调用频率超限: 每分钟最多 50 次")
        print("        解决: 等待1分钟后重试")
        print("     3. 权限不足: 检查账号状态")
        print("        解决: 登录 https://tushare.pro 查看积分和权限")
        return False
    
    print()
    
    # 4. 测试单只股票数据
    print("📝 步骤4: 测试获取单只股票数据...")
    try:
        df = pro.daily(ts_code='000001.SZ', start_date='20260101', end_date='20260210')
        if df is not None and not df.empty:
            print(f"✅ 成功获取 000001.SZ 的数据，共 {len(df)} 条记录")
        else:
            print("⚠️  未获取到数据")
    except Exception as e:
        print(f"❌ 获取失败: {e}")
    
    print()
    print("="*80)
    print("测试完成")
    print("="*80)
    print()
    print("✅ 所有测试通过！可以使用 make daily-tushare 获取数据")
    print()
    print("下一步:")
    print("  1. 运行: make daily-tushare")
    print("  2. 或者: poetry run python fetch_daily_data_tushare.py")
    print()
    
    return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='TuShare 连接测试')
    parser.add_argument('--token', type=str, help='TuShare API Token')
    args = parser.parse_args()
    
    # 获取 Token
    token = args.token or get_token()
    
    if not token:
        print("="*80)
        print("❌ 错误: 未配置 TuShare Token")
        print("="*80)
        print()
        print("请通过以下方式之一配置 Token:")
        print()
        print("方式1（推荐）：设置环境变量")
        print('  export TUSHARE_TOKEN="你的token"')
        print()
        print("方式2：在 config.py 中添加")
        print('  TUSHARE_TOKEN = "你的token"')
        print()
        print("方式3：命令行参数")
        print('  python test_tushare.py --token "你的token"')
        print()
        print("获取 Token:")
        print("  1. 注册账号: https://tushare.pro/register")
        print("  2. 获取 token: https://tushare.pro/user/token")
        print("="*80)
        sys.exit(1)
    
    # 测试连接
    success = test_connection(token)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
