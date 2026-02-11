# 网络问题说明

## 问题现象

运行 `make daily` 时出现以下错误：

```
HTTPSConnectionPool(host='82.push2.eastmoney.com', port=443): Max retries exceeded
ProxyError('Unable to connect to proxy', RemoteDisconnected(...))
```

## 原因分析

### 1. fetch_daily_data.py 的设计

- **数据源**：直接使用 **akshare** 的 `stock_zh_a_spot_em()` 接口
- **优势**：批量获取，速度快（5-10秒完成）
- **劣势**：依赖东方财富服务器（`82.push2.eastmoney.com`），可能需要代理

### 2. fetch_historical_data.py 的设计

- **数据源**：使用 `MultiSourceFetcher`，优先 **baostock**
- **优势**：多数据源降级，网络稳定性好
- **劣势**：逐个股票获取，速度慢（2-3小时）

## 解决方案

### 方案1：配置代理（推荐）

如果你的网络环境需要代理才能访问东方财富：

```bash
# 设置代理
export http_proxy=http://your-proxy:port
export https_proxy=http://your-proxy:port

# 然后运行
make daily
```

### 方案2：使用 fetch_historical_data.py

如果无法配置代理，使用更稳定但较慢的方式：

```bash
# 使用 baostock 获取数据（更稳定）
make history
```

**优点**：
- ✅ 使用 baostock，网络更稳定
- ✅ 多数据源降级机制
- ✅ 不需要代理

**缺点**：
- ❌ 速度较慢（2-3小时 vs 5-10秒）
- ❌ 需要逐个股票轮询

### 方案3：检查网络连接

```bash
# 测试是否能访问东方财富服务器
curl -I https://82.push2.eastmoney.com

# 如果失败，检查代理设置
echo $http_proxy
echo $https_proxy
```

## 脚本对比

| 脚本 | 数据源 | 速度 | 网络要求 | 适用场景 |
|------|--------|------|---------|---------|
| `fetch_daily_data.py` | akshare (东方财富) | 快 (5-10秒) | 可能需要代理 | 网络良好时的日常更新 |
| `fetch_historical_data.py` | baostock (优先) | 慢 (2-3小时) | 无特殊要求 | 网络受限或首次获取 |

## 建议

1. **日常更新**：如果网络良好，使用 `make daily`
2. **网络受限**：使用 `make history`（虽然慢但稳定）
3. **混合使用**：
   - 首次获取：`make history`（完整历史数据）
   - 日常更新：尝试 `make daily`，失败则回退到 `make history`

## 未来改进

可以考虑为 `fetch_daily_data.py` 添加：
1. 自动重试机制
2. 降级到逐个股票获取（使用 baostock）
3. 代理自动检测和配置
