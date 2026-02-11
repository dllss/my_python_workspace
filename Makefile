.PHONY: list history daily single install update clean check-date check-sources rebuild-metadata analyze-names clean-suspended

# 获取股票列表（第一步）
list:
	@poetry run python fetch_stock_list.py

# 批量获取历史数据（第二步，首次使用）
history:
	@poetry run python fetch_historical_data.py

# 每日增量更新（日常使用，推荐）
daily:
	@poetry run python fetch_daily_data.py

# 获取单只股票数据
# 用法: make single CODE=000001 或 make single CODE=600519 START=20240101 END=20240131
single:
	@poetry run python fetch_single_stock.py $(CODE) $(if $(START),--start $(START),) $(if $(END),--end $(END),)

# 检查指定日期是否为交易日
# 用法: make check-date DATE=20000103
check-date:
	@poetry run python scripts/check_trading_day.py $(DATE)

# 检查数据源可用性
check-sources:
	@poetry run python scripts/check_data_sources.py

# 重建元数据文件（提高性能）
rebuild-metadata:
	@poetry run python scripts/rebuild_metadata.py

# 分析股票名称变化历史
# 用法: make analyze-names 或 make analyze-names CODE=000001
analyze-names:
	@poetry run python scripts/analyze_stock_name_changes.py $(if $(CODE),--code $(CODE),)

# 清理停牌数据
# 用法: make clean-suspended 或 make clean-suspended CLEAN=1
clean-suspended:
	@poetry run python scripts/clean_suspended_data.py $(if $(CLEAN),--clean,)

# 安装依赖
install:
	@poetry install

# 更新所有依赖（包括交易日历）
update:
	@echo "正在更新依赖..."
	@poetry update
	@echo "✅ 依赖已更新"

# 清理缓存文件
clean:
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "清理完成"
