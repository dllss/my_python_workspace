# -*- coding: utf-8 -*-
"""
YFinance 数据源模块
提供 A 股历史数据获取功能（备用数据源）
"""

import sys
import os
import pandas as pd
import yfinance as yf


class YFinanceFetcher:
    """YFinance 数据获取器"""
    
    def __init__(self):
        """初始化 yfinance fetcher"""
        pass
    
    def is_available(self):
        """
        检查 yfinance 数据源是否可用
        通过获取一个测试股票的最近1天数据来验证
        
        Returns:
            bool: True 表示可用，False 表示不可用
        """
        try:
            # 使用贵州茅台(600519.SS)作为测试股票
            from datetime import datetime, timedelta
            test_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # 禁用输出
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = open(os.devnull, 'w')
            sys.stderr = open(os.devnull, 'w')
            
            try:
                df = yf.download("600519.SS", start=test_date, end=test_date, progress=False)
                return True
            finally:
                sys.stdout.close()
                sys.stderr.close()
                sys.stdout = old_stdout
                sys.stderr = old_stderr
        except Exception:
            return False
    
    def fetch(self, stock_code, start_date, end_date):
        """
        从 yfinance 获取数据并计算缺失字段
        
        Args:
            stock_code: 股票代码（6位，如 '000001'）
            start_date: 开始日期（格式：'20260201'）
            end_date: 结束日期（格式：'20260210'）
        
        Returns:
            DataFrame: 标准化后的数据，列名与 akshare 一致
            None: 如果获取失败或无数据
        """
        try:
            # 转换日期格式：20260201 -> 2026-02-01
            start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
            end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
            
            # 转换股票代码格式
            if stock_code.startswith('6'):
                yf_code = f"{stock_code}.SS"  # 上海
            else:
                yf_code = f"{stock_code}.SZ"  # 深圳
            
            # 禁用 yfinance 的输出
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = open(os.devnull, 'w')
            sys.stderr = open(os.devnull, 'w')
            
            try:
                df = yf.download(yf_code, start=start, end=end, auto_adjust=True, progress=False)
            finally:
                # 恢复输出
                sys.stdout.close()
                sys.stderr.close()
                sys.stdout = old_stdout
                sys.stderr = old_stderr
            
            if df.empty:
                return None
            
            # 重置索引
            df = df.reset_index()
            
            # 处理 MultiIndex 列名（yfinance 可能返回 MultiIndex）
            if isinstance(df.columns, pd.MultiIndex):
                # 只保留第一层列名
                df.columns = df.columns.get_level_values(0)
            
            # 计算缺失字段
            # 1. 涨跌幅 = (收盘价 - 昨日收盘价) / 昨日收盘价 * 100
            df['pct_change'] = ((df['Close'] - df['Close'].shift(1)) / df['Close'].shift(1) * 100).fillna(0)
            
            # 2. 涨跌额 = 收盘价 - 昨日收盘价
            df['change'] = (df['Close'] - df['Close'].shift(1)).fillna(0)
            
            # 3. 振幅 = (最高价 - 最低价) / 昨日收盘价 * 100
            prev_close = df['Close'].shift(1)
            df['amplitude'] = ((df['High'] - df['Low']) / prev_close * 100).fillna(0)
            
            # 标准化列名（与 akshare 保持一致）
            df = df.rename(columns={
                'Date': '日期',
                'Open': '开盘',
                'High': '最高',
                'Low': '最低',
                'Close': '收盘',
                'Volume': '成交量'
            })
            
            # 统一数据格式
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            
            # 添加字段
            df['股票代码'] = stock_code
            
            # 确保股票代码为字符串类型（保留前导零）
            df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
            
            df['成交额'] = 0.0  # yfinance 不提供成交额
            df['涨跌幅'] = df['pct_change'].round(2)
            df['涨跌额'] = df['change'].round(2)
            df['振幅'] = df['amplitude'].round(2)
            df['换手率'] = 0.0  # yfinance 不提供换手率
            
            # 删除临时列
            df = df.drop(columns=['pct_change', 'change', 'amplitude'], errors='ignore')
            
            # 统一数值精度（保留2位小数）
            numeric_cols = ['开盘', '收盘', '最高', '最低']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].round(2)
            
            # 调整列顺序，与 akshare 一致
            column_order = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
            df = df[[col for col in column_order if col in df.columns]]
            
            return df
            
        except Exception as e:
            return None
