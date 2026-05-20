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

股票画像同步使用 JoinQuant/JQData 官方数据库拉取一级行业和概念板块成分，并写入本地 SQLite 的 `stock_boards` 与 `stock_board_members`。WebUI 的行业板块和概念板块筛选项直接读取这两张本地表。

在项目根目录 `.env` 中填写：

```env
JQDATA_USERNAME=你的JoinQuant账号
JQDATA_PASSWORD=你的JoinQuant密码
JQDATA_INDUSTRY=jq_l1
```

请只把真实账号密码填到 `.env`，不要填到 `.env.example`；`.env.example` 是 Git 跟踪的模板文件。

`JQDATA_INDUSTRY=jq_l1` 表示聚宽一级行业；如需申万一级行业，可改为 `sw_l1`。概念板块使用 JQData 的概念分类列表。`data/stock_board_mapping.example.csv` 仅保留为人工维护映射时的参考样例，当前“同步股票画像”不读取该文件。

---

## 许可证 | License

MIT
