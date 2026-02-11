"""
元数据管理器

用于管理股票数据的元数据（如最新日期），提高增量更新的性能
"""

import os
import json
import logging
from typing import Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class MetadataManager:
    """
    元数据管理器
    
    管理每只股票的最新日期，避免每次都读取CSV文件
    """
    
    def __init__(self, data_dir: str):
        """
        初始化元数据管理器
        
        Args:
            data_dir: 数据目录路径
        """
        self.data_dir = data_dir
        self.metadata_file = os.path.join(data_dir, '.metadata.json')
        self._cache: Optional[Dict[str, str]] = None
    
    def _load_metadata(self) -> Dict[str, str]:
        """
        加载元数据
        
        Returns:
            Dict[str, str]: 元数据字典 {"股票代码": "最新日期", ...}
        """
        # 使用缓存
        if self._cache is not None:
            return self._cache
        
        try:
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                self._cache = json.load(f)
            # 成功加载，不输出日志（避免频繁打印）
            return self._cache
        except Exception as e:
            logger.warning(f"加载元数据失败: {e}，使用空元数据")
            self._cache = {}
            return self._cache
    
    def _save_metadata(self, metadata: Dict[str, str]) -> None:
        """
        保存元数据到文件
        
        Args:
            metadata: 元数据字典
        """
        import time
        
        max_retries = 3
        retry_delay = 0.1  # 100ms
        
        for attempt in range(max_retries):
            try:
                # 确保目录存在
                os.makedirs(self.data_dir, exist_ok=True)
                
                # 使用进程ID和时间戳生成唯一的临时文件名（避免并发冲突）
                import random
                temp_file = f"{self.metadata_file}.tmp.{os.getpid()}.{random.randint(1000, 9999)}"
                
                # 写入临时文件
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                
                # 确保文件已写入
                if not os.path.exists(temp_file):
                    raise FileNotFoundError(f"临时文件创建失败: {temp_file}")
                
                # 原子替换（os.replace 在所有平台上都支持原子覆盖）
                os.replace(temp_file, self.metadata_file)
                
                # 成功保存，不输出日志（避免频繁打印）
                return  # 成功保存，退出
                
            except Exception as e:
                # 清理临时文件
                try:
                    if 'temp_file' in locals() and os.path.exists(temp_file):
                        os.remove(temp_file)
                except:
                    pass
                
                # 如果是最后一次尝试，记录错误
                if attempt == max_retries - 1:
                    logger.error(f"保存元数据失败（重试{max_retries}次后）: {e}")
                else:
                    # 短暂延迟后重试
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
    
    def get_last_date(self, stock_code: str) -> Optional[str]:
        """
        获取股票的最新日期
        
        Args:
            stock_code: 股票代码
        
        Returns:
            str: 最新日期（格式 YYYYMMDD），如果不存在返回 None
        """
        metadata = self._load_metadata()
        return metadata.get(stock_code)
    
    def update_last_date(self, stock_code: str, last_date: str) -> None:
        """
        更新股票的最新日期
        
        Args:
            stock_code: 股票代码
            last_date: 最新日期（格式 YYYYMMDD）
        """
        metadata = self._load_metadata()
        metadata[stock_code] = last_date
        self._cache = metadata
        self._save_metadata(metadata)
    
    def batch_update(self, updates: Dict[str, str]) -> None:
        """
        批量更新多个股票的最新日期
        
        Args:
            updates: 更新字典 {"股票代码": "最新日期", ...}
        """
        metadata = self._load_metadata()
        metadata.update(updates)
        self._cache = metadata
        self._save_metadata(metadata)
        logger.info(f"批量更新元数据: {len(updates)} 只股票")
    
    def remove_stock(self, stock_code: str) -> None:
        """
        从元数据中移除股票
        
        Args:
            stock_code: 股票代码
        """
        metadata = self._load_metadata()
        if stock_code in metadata:
            del metadata[stock_code]
            self._cache = metadata
            self._save_metadata(metadata)
    
    def clear(self) -> None:
        """
        清空元数据
        """
        self._cache = {}
        if os.path.exists(self.metadata_file):
            os.remove(self.metadata_file)
        logger.info("元数据已清空")
    
    def rebuild_from_files(self, stock_codes: list) -> int:
        """
        从现有的 CSV 文件重建元数据
        
        Args:
            stock_codes: 股票代码列表
        
        Returns:
            int: 成功重建的股票数量
        """
        metadata = {}
        success_count = 0
        
        for stock_code in stock_codes:
            try:
                file_path = os.path.join(self.data_dir, f"stock_{stock_code}.csv")
                if not os.path.exists(file_path):
                    continue
                
                # 只读取最后一行（最新日期）
                df = pd.read_csv(file_path, dtype={'股票代码': str})
                if not df.empty:
                    # 确保日期列是日期类型
                    df['日期'] = pd.to_datetime(df['日期'])
                    last_date = df['日期'].max().strftime('%Y%m%d')
                    metadata[stock_code] = last_date
                    success_count += 1
            except Exception as e:
                logger.debug(f"重建 {stock_code} 元数据失败: {e}")
                continue
        
        if metadata:
            self._cache = metadata
            self._save_metadata(metadata)
            logger.info(f"元数据重建完成: {success_count}/{len(stock_codes)} 只股票")
        
        return success_count
    
    def get_stats(self) -> Dict:
        """
        获取元数据统计信息
        
        Returns:
            Dict: 统计信息
        """
        metadata = self._load_metadata()
        return {
            'total': len(metadata),
            'file_exists': os.path.exists(self.metadata_file)
        }
