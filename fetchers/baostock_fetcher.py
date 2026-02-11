# -*- coding: utf-8 -*-
"""
Baostock 数据源模块
提供 A 股历史数据获取功能
"""

import sys
import os
import pandas as pd
import baostock as bs


class BaostockFetcher:
    """Baostock 数据获取器"""
    
    def __init__(self):
        """初始化并登录 baostock"""
        self.is_logged_in = False
    
    def is_available(self):
        """
        检查 baostock 数据源是否可用
        通过尝试登录来验证
        
        Returns:
            bool: True 表示可用，False 表示不可用
        """
        try:
            if self.is_logged_in:
                return True
            return self.login(silent=True)
        except Exception:
            return False
    
    def login(self, silent=False):
        """
        登录 baostock
        
        Args:
            silent: 是否静默登录（不打印消息）
        """
        if not self.is_logged_in:
            # 禁用 baostock 的输出
            if silent:
                old_stdout = sys.stdout
                sys.stdout = open(os.devnull, 'w')
            
            try:
                lg = bs.login()
                if lg.error_code == '0':
                    self.is_logged_in = True
                    return True
                else:
                    if not silent:
                        print(f"login failed: {lg.error_msg}")
                    return False
            finally:
                # 恢复输出
                if silent:
                    sys.stdout.close()
                    sys.stdout = old_stdout
        return True
    
    def logout(self, silent=False):
        """
        登出 baostock
        
        Args:
            silent: 是否静默登出（不打印消息）
        """
        if self.is_logged_in:
            # 禁用 baostock 的输出
            if silent:
                old_stdout = sys.stdout
                sys.stdout = open(os.devnull, 'w')
            
            try:
                bs.logout()
            finally:
                # 恢复输出
                if silent:
                    sys.stdout.close()
                    sys.stdout = old_stdout
            
            self.is_logged_in = False
    
    def fetch(self, stock_code, start_date, end_date):
        """
        从 baostock 获取数据并计算缺失字段
        
        Args:
            stock_code: 股票代码（6位，如 '000001'）
            start_date: 开始日期（格式：'20260201'）
            end_date: 结束日期（格式：'20260210'）
        
        Returns:
            DataFrame: 标准化后的数据，列名与 akshare 一致
            None: 如果获取失败或无数据
        """
        # 确保已登录
        if not self.is_logged_in:
            if not self.login(silent=True):
                return None
        
        # 转换日期格式：20260201 -> 2026-02-01
        start = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
        end = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
        
        # 转换股票代码格式：000001 -> sz.000001, 600519 -> sh.600519
        if stock_code.startswith('6'):
            bs_code = f"sh.{stock_code}"
        else:
            bs_code = f"sz.{stock_code}"
        
        # 查询数据
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,code,open,high,low,close,volume,amount,pctChg,turn",
            start_date=start,
            end_date=end,
            frequency="d",
            adjustflag="2"  # 2:前复权
        )
        
        # 提取数据
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            return None
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 转换数值类型
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pctChg', 'turn']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # 计算缺失字段
        # 1. 涨跌额 = 收盘价 - 昨日收盘价
        df['change'] = (df['close'] - df['close'].shift(1)).fillna(0)
        
        # 2. 振幅 = (最高价 - 最低价) / 昨日收盘价 * 100
        prev_close = df['close'].shift(1)
        df['amplitude'] = ((df['high'] - df['low']) / prev_close * 100).fillna(0)
        
        # 标准化列名（与 akshare 保持一致）
        df = df.rename(columns={
            'date': '日期',
            'code': '股票代码',
            'open': '开盘',
            'close': '收盘',
            'high': '最高',
            'low': '最低',
            'volume': '成交量',
            'amount': '成交额',
            'pctChg': '涨跌幅',
            'change': '涨跌额',
            'amplitude': '振幅',
            'turn': '换手率'
        })
        
        # 统一数据格式
        df['股票代码'] = stock_code  # 统一为6位代码
        df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        # 确保股票代码为字符串类型（保留前导零）
        df['股票代码'] = df['股票代码'].astype(str).str.zfill(6)
        
        # 统一数值精度（保留2位小数）
        numeric_cols = ['开盘', '收盘', '最高', '最低', '涨跌幅', '涨跌额', '振幅', '换手率']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].round(2)
        
        # 调整列顺序，与 akshare 一致
        column_order = ['日期', '股票代码', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        df = df[[col for col in column_order if col in df.columns]]
        
        return df
    
    def __enter__(self):
        """上下文管理器：进入"""
        self.login(silent=True)  # 静默登录
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器：退出"""
        self.logout(silent=True)  # 静默登出
        return False
