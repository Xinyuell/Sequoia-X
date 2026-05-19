# Sequoia-X: 王者回归 | The King Returns

> A 股量化选股系统 

---

## 简介 | Introduction

Sequoia-X V2 是面向 A 股市场的量化选股系统，基于现代 Python 工程化标准从零重构。
系统以 OOP 架构、向量化计算和增量数据更新为核心设计原则，每日收盘后自动选股并推送至飞书群。

数据层使用 [baostock](http://baostock.com)拉取历史及增量日 K 数据（后复权），存储于本地 SQLite，备用Tushare数据

通过webUI进行详细的策略运行，调整不同参数，选股并参看。
---


## 目录结构 | Project Structure

```
Sequoia-X/
├── main.py                      # 入口：argparse 分发日常/回填模式
├── pyproject.toml               # 依赖声明 + ruff/pytest 配置
├── .env.example                 # 环境变量模板
├── data/                        # SQLite 数据库（运行时生成，不入 git）
├── sequoia_x/
│   ├── core/
│   │   ├── config.py            # Pydantic-settings 配置管理
│   │   └── logger.py            # rich 结构化日志
│   ├── data/
│   │   └── engine.py            # 数据引擎（baostock 回填 + 增量同步 + SQLite）
│   ├── strategy/
│   │   ├── base.py              # 策略抽象基类
│   │   ├── turtle_trade.py      # 海龟交易策略
│   │   ├── ma_volume.py         # 均线放量策略
│   │   ├── high_tight_flag.py   # 高窄旗形策略
│   │   ├── limit_up_shakeout.py # 涨停洗盘策略
│   │   ├── uptrend_limit_down.py # 上升跌停策略
│   │   └── rps_breakout.py      # RPS 突破策略
│   └── notify/
│       └── feishu.py            # 飞书 Webhook 推送
└── tests/                       # 属性测试（hypothesis）
```

---

## 数据说明

- **数据源**：[baostock](http://baostock.com)（免费、无需注册、无限流）
- **复权方式**：后复权（hfq）— 历史价格不变，适合增量存储，避免除权导致数据错乱
- **存储**：本地 SQLite（`data/sequoia_v2.db`），可直接拷贝到其他机器使用
- **日常增量**：8 进程并行通过 baostock 拉取，2~3 分钟完成全市场更新

### 行业/概念板块映射

网页爬虫源不稳定时，可以使用本地 CSV 作为股票池筛选的行业和概念映射来源。默认路径是 `data/stock_board_mapping.csv`，也可以通过 `.env` 中的 `BOARD_MAPPING_PATH` 修改。

CSV 至少包含以下列：

```csv
symbol,name,一级行业,概念
000001,平安银行,银行,大金融;低价股
600519,贵州茅台,食品饮料,白酒;核心资产
```

支持的列名包括 `symbol`/`股票代码`/`代码`，`一级行业`/`行业`，以及 `概念`/`概念板块`/`concepts`。概念多个值可用 `;`、`,`、`，`、`、` 分隔。同步股票画像时会优先读取这个本地文件；如果文件不存在，再尝试 Tushare Pro 的行业/概念正式 API。

---

## 许可证 | License

MIT
