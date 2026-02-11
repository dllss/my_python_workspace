# -*- coding: utf-8 -*-
"""
Akshare 数据源模块
提供 A 股历史数据获取功能
"""

import pandas as pd
import akshare as ak


class AkshareFetcher:
    """Akshare 数据获取器"""
    
    def __init__(self):
        """初始化 akshare fetcher"""
        pass
    
    def is_available(self):
        """
        检查 akshare 数据源是否可用
        通过获取一个测试股票的最近1天数据来验证
        
        Returns:
            bool: True 表示可用，False 表示不可用
        """
        try:
            # 使用贵州茅台(600519)作为测试股票，获取最近1天数据
            from datetime import datetime, timedelta
            test_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            df = ak.stock_zh_a_hist(
                symbol="600519",
                period="daily",
                start_date=test_date,
                end_date=test_date,
                adjust="qfq"
            )
            return True
        except Exception:
            return False
    
    def fetch(self, stock_code, start_date, end_date, adjust_type='qfq'):
        """
        从 akshare 获取数据（保留原始字段）
        
        Args:
            stock_code: 股票代码（6位，如 '000001'）
            start_date: 开始日期（格式：'20260201'）
            end_date: 结束日期（格式：'20260210'）
            adjust_type: 复权类型，'qfq'(前复权), 'hfq'(后复权), ''(不复权)
        
        Returns:
            DataFrame: 标准化后的数据
            None: 如果获取失败或无数据
        """
        try:
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=adjust_type
            )
            
            if df.empty:
                return None
            
            # 保持 akshare 原始列名，只做格式统一
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            df['股票代码'] = stock_code
            
            # 确保股票代码为字符串类型（保留前导零）
            df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
            
            # 填充空值（第一行可能没有涨跌额、振幅等数据）
            df['涨跌额'] = df['涨跌额'].fillna(0)
            df['振幅'] = df['振幅'].fillna(0)
            
            # 统一数值精度（保留2位小数）
            numeric_cols = ['开盘', '收盘', '最高', '最低', '涨跌幅', '涨跌额', '振幅', '换手率']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = df[col].round(2)
            
            return df
            
        except Exception as e:
            return None
