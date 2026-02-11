# -*- coding: utf-8 -*-
"""
数据获取器模块
提供多种股票数据获取接口
"""

from .akshare_fetcher import AkshareFetcher
from .baostock_fetcher import BaostockFetcher
from .yfinance_fetcher import YFinanceFetcher
from .multi_source_fetcher import MultiSourceFetcher, FetchResult

__all__ = ['AkshareFetcher', 'BaostockFetcher', 'YFinanceFetcher', 'MultiSourceFetcher', 'FetchResult']
