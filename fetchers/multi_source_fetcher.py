# -*- coding: utf-8 -*-
"""
多数据源管理器
提供自动降级的数据获取策略
"""

import logging
from typing import NamedTuple, Optional
import pandas as pd
from .akshare_fetcher import AkshareFetcher
from .baostock_fetcher import BaostockFetcher
from .yfinance_fetcher import YFinanceFetcher


# 定义返回结果的命名元组（使用 typing.NamedTuple 更现代）
class FetchResult(NamedTuple):
    data: Optional[pd.DataFrame]
    source: Optional[str]

# 配置日志
logger = logging.getLogger(__name__)


class MultiSourceFetcher:
    """
    多数据源获取器
    按优先级自动尝试不同数据源
    
    默认优先级：baostock -> akshare -> yfinance
    可自定义优先数据源
    """
    
    def __init__(self, preferred_source: str = 'baostock'):
        """
        初始化所有数据源
        
        Args:
            preferred_source: 优先使用的数据源
                - 'baostock': BaoStock（默认，数据最完整）
                - 'akshare': AkShare（速度快，但可能被限流）
                - 'yfinance': YFinance（国际数据源，A股支持有限）
        """
        self.akshare_fetcher = AkshareFetcher()
        self.baostock_fetcher = None  # 延迟初始化，使用时才创建
        self.yfinance_fetcher = YFinanceFetcher()
        self.source_stats = {"akshare": 0, "baostock": 0, "yfinance": 0}
        
        # 设置优先数据源
        if preferred_source not in ['baostock', 'akshare', 'yfinance']:
            logger.warning(f"⚠️  无效的数据源: {preferred_source}，使用默认值 'baostock'")
            preferred_source = 'baostock'
        
        self.preferred_source = preferred_source
    
    def check_sources(self):
        """
        检查所有数据源的可用性
        
        Returns:
            dict: 各数据源的可用状态 {'akshare': True/False, 'baostock': True/False, 'yfinance': True/False}
        """
        status = {}
        
        # 检查 akshare
        try:
            status['akshare'] = self.akshare_fetcher.is_available()
            if status['akshare']:
                logger.debug("✅ Akshare 数据源可用")
            else:
                logger.warning("⚠️  Akshare 数据源不可用")
        except Exception as e:
            status['akshare'] = False
            logger.error(f"❌ Akshare 数据源检查失败: {str(e)[:100]}")
        
        # 检查 baostock
        try:
            if self.baostock_fetcher is None:
                self.baostock_fetcher = BaostockFetcher()
            status['baostock'] = self.baostock_fetcher.is_available()
            if status['baostock']:
                logger.debug("✅ Baostock 数据源可用")
            else:
                logger.warning("⚠️  Baostock 数据源不可用")
        except Exception as e:
            status['baostock'] = False
            logger.error(f"❌ Baostock 数据源检查失败: {str(e)[:100]}")
        
        # 检查 yfinance
        try:
            status['yfinance'] = self.yfinance_fetcher.is_available()
            if status['yfinance']:
                logger.debug("✅ YFinance 数据源可用")
            else:
                logger.warning("⚠️  YFinance 数据源不可用")
        except Exception as e:
            status['yfinance'] = False
            logger.error(f"❌ YFinance 数据源检查失败: {str(e)[:100]}")
        
        return status
    
    def fetch(self, stock_code, stock_name, start_date, end_date, adjust_type='qfq'):
        """
        多数据源获取策略
        按优先级尝试：baostock -> akshare -> yfinance
        所有数据源返回统一的格式
        
        Args:
            stock_code: 股票代码（6位，如 '000001'）
            stock_name: 股票名称
            start_date: 开始日期（格式：'20260201'）
            end_date: 结束日期（格式：'20260210'）
            adjust_type: 复权类型，'qfq'(前复权), 'hfq'(后复权), ''(不复权)
        
        Returns:
            FetchResult: 命名元组，包含 data 和 source 两个字段
                - result.data: DataFrame 或 None
                - result.source: 数据源名称
                    - 'baostock', 'akshare', 'yfinance': 成功获取数据的数据源
                    - 'no_data': 数据源正常但无数据（节假日/停牌/未上市等）
                    - None: 所有数据源都失败（接口异常）
            
        Examples:
            >>> result = fetcher.fetch('000001', '平安银行', '20260211', '20260211')
            >>> if result.data is not None:
            >>>     print(f"成功获取数据，来源：{result.source}")
            >>> elif result.source == "no_data":
            >>>     print("无数据（节假日/停牌/未上市等）")
            >>> else:
            >>>     print("所有数据源均失败")
        """
        # 定义所有可用的数据源
        all_sources = {
            "baostock": lambda: self._fetch_from_baostock(stock_code, start_date, end_date),
            "akshare": lambda: self._fetch_from_akshare(stock_code, start_date, end_date, adjust_type),
            "yfinance": lambda: self._fetch_from_yfinance(stock_code, start_date, end_date)
        }
        
        # 根据优先数据源构建尝试顺序
        sources = []
        
        # 1. 首先添加优先数据源
        if self.preferred_source in all_sources:
            sources.append((self.preferred_source, all_sources[self.preferred_source]))
        
        # 2. 然后添加其他数据源（作为备用）
        for source_name, fetch_func in all_sources.items():
            if source_name != self.preferred_source:
                sources.append((source_name, fetch_func))
        
        # 按优先级尝试每个数据源
        has_exception = False  # 标记是否有数据源抛出异常
        failed_sources = []  # 记录失败的数据源及原因
        
        for source_name, fetch_func in sources:
            try:
                df = fetch_func()
                if df is not None and not df.empty:
                    # 成功获取到数据（不打印日志，保持简洁）
                    
                    # 添加股票名称列（插入到第2列）
                    if '股票名称' not in df.columns:
                        df.insert(1, '股票名称', stock_name)
                    
                    # 统计数据源使用情况
                    self.source_stats[source_name] += 1
                    
                    return FetchResult(data=df, source=source_name)
                else:
                    # 数据源正常返回，但数据为空
                    # 说明这段时间确实没有数据（节假日/停牌/未上市等），直接返回，不再尝试其他数据源
                    logger.debug(f"    ▶️ 尝试从 {source_name} 获取 {stock_code} 的数据 => ℹ️  无数据")
                    return FetchResult(data=None, source="no_data")
            except Exception as e:
                # 数据源异常，尝试下一个
                has_exception = True
                error_msg = str(e)[:100]
                failed_sources.append((source_name, error_msg))
                logger.warning(f"    ▶️ 尝试从 {source_name} 获取 {stock_code} 的数据 => ⚠️  失败: {error_msg}")
                continue
        
        # 所有数据源都尝试完毕
        # 如果有异常发生，返回失败状态
        if has_exception:
            logger.error(f"    ❌ 所有数据源均失败 (股票: {stock_code} {stock_name}, 日期: {start_date}~{end_date})")
            # 输出详细的失败信息
            for source_name, error_msg in failed_sources:
                logger.debug(f"       - {source_name}: {error_msg}")
            return FetchResult(data=None, source=None)
        else:
            # 理论上不会走到这里（因为上面已经处理了空数据的情况）
            logger.debug(f"    ℹ️  无数据可获取 (股票: {stock_code}, 日期: {start_date}~{end_date})")
            return FetchResult(data=None, source="no_data")
    
    def _fetch_from_akshare(self, stock_code, start_date, end_date, adjust_type):
        """从 akshare 获取数据"""
        return self.akshare_fetcher.fetch(stock_code, start_date, end_date, adjust_type)
    
    def _fetch_from_baostock(self, stock_code, start_date, end_date):
        """从 baostock 获取数据"""
        # 延迟初始化 baostock（因为需要登录）
        if self.baostock_fetcher is None:
            self.baostock_fetcher = BaostockFetcher()
            self.baostock_fetcher.login(silent=True)  # 静默登录
        
        return self.baostock_fetcher.fetch(stock_code, start_date, end_date)
    
    def _fetch_from_yfinance(self, stock_code, start_date, end_date):
        """从 yfinance 获取数据"""
        return self.yfinance_fetcher.fetch(stock_code, start_date, end_date)
    
    def get_stats(self):
        """
        获取数据源使用统计
        
        Returns:
            dict: 各数据源的使用次数
        """
        return self.source_stats.copy()
    
    def reset_stats(self):
        """重置统计数据"""
        self.source_stats = {"akshare": 0, "baostock": 0, "yfinance": 0}
    
    def close(self):
        """关闭所有连接"""
        if self.baostock_fetcher is not None and self.baostock_fetcher.is_logged_in:
            self.baostock_fetcher.logout(silent=True)  # 静默登出
    
    def __enter__(self):
        """上下文管理器：进入"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器：退出"""
        self.close()
        return False
