# 招投标文件核心 Pipeline

服务器部署目录：

```text
/home/wisdi/projects/bidding_files_utils
```

本项目是招投标 PDF 文件处理的核心 Pipeline，提供 CLI 和 Web 两种入口，统一编排以下流程：

```text
PDF 输入
  ↓
OCR 解析与分类
  ↓
企业名称清洗、企业爬虫提交与结果核验
  ↓
解析结果幂等写入 openGauss
  ↓
通过 openGauss SQL 查询并分析关联风险
  ↓
生成风险 JSON、Markdown 和 PDF 报告
```

当前部署版不使用 Neo4j 进行风险查询，也不包含旧版
`biding_files_extraction/web_app/services/job_service.py` 任务编排逻辑。
完整流程编排以 `bidding_files_pipeline/bidding_pipeline` 为唯一核心入口。

## 目录结构

```text
bidding_files_utils/
├─ bidding_files/                    # 服务器允许处理的原始招投标文件目录
├─ bidding_files_extraction/         # OCR 策略源码
├─ bidding_files_extraction_mp/      # OCR 多进程版本及测试代码
├─ bidding_files_ocr_strategies/     # OCR 策略源码兼容目录
├─ bidding_files_pipeline/           # 核心 Pipeline
│  ├─ bidding_pipeline/
│  │  ├─ cli.py                      # CLI 参数和入口
│  │  ├─ runner.py                   # 唯一主流程编排器
│  │  ├─ database.py                 # openGauss/PostgreSQL 兼容数据库访问
│  │  ├─ records.py                  # final.csv 记录读取和标准化
│  │  ├─ spider.py                   # 企业爬虫客户端和调度器
│  │  ├─ risk_analysis.py             # 基于 SQL 查询的风险分析
│  │  ├─ reporting.py                # 风险报告上下文和报告生成
│  │  ├─ web.py                      # FastAPI Web 服务和任务管理
│  │  ├─ templates/                  # Web 页面模板
│  │  └─ static/                     # Web 静态资源
│  ├─ tests/                         # Pipeline 测试
│  ├─ deploy/                        # systemd 服务模板
│  ├─ pyproject.toml                 # Python 项目定义
│  └─ requirements.txt               # 运行依赖
├─ biding_files_risk_reports/        # Markdown 转 PDF 的报告渲染脚本和模板
├─ logs/                             # 运行日志
└─ output/                           # 运行输出
```

## 核心模块职责

### `bidding_pipeline.runner`

`runner.py` 是唯一的完整流程编排入口，公开的核心函数是：

```python
from bidding_pipeline import run_pipeline
```

一次完整运行按以下阶段执行：

1. 动态加载 OCR 策略包中的 `ProcessingConfig` 和 `process_pdf_tree`。
2. 解析输入目录下的 PDF，并在 PDF 完成解析后将企业名称提交给爬虫调度器。
3. 等待企业爬虫任务收尾，生成爬虫审计结果。
4. 读取 `final.csv`，将解析结果幂等写入 openGauss。
5. 从 openGauss 查询本次 `run_id` 的投标结果和企业详情，分析关联风险。
6. 生成风险 Markdown，并调用报告渲染脚本生成 PDF。
7. 写入 `run_manifest.json`，保存本次运行的非敏感配置和结果摘要。

OCR、爬虫、数据库、风险分析和报告生成是能力模块；流程顺序、运行状态、异常处理、重试和 Web 展示由 `runner.py`、`web.py` 和 `cli.py` 统一管理。

### `database.py`

数据库模块使用 `psycopg2` 或兼容驱动访问 openGauss/PostgreSQL，主要负责：

- 创建或检查解析结果表；
- 将 `final.csv` 按业务键幂等 Upsert；
- 校验爬虫企业数据是否已经落库；
- 为风险分析提供数据库连接配置。

默认解析结果表为：

```text
dwd.dwd_bid_extraction_results
```

### `spider.py`

该模块负责调用企业爬虫服务并轮询任务状态，支持：

- 按企业逐个提交；
- 企业名称去重和清洗；
- 根企业与关联企业结果区分；
- 结果状态归一化；
- 重试、停滞检测和后续对账；
- 生成 `crawl_results.json`。

默认爬虫服务地址为：

```text
http://192.168.1.166:9081
```

建议通过环境变量覆盖，不要在业务代码中修改地址。

### `risk_analysis.py`

风险分析只查询 openGauss，不访问 Neo4j。

主要查询表包括：

```text
dwd_bid_extraction_results
spider_data_person_enterprise_relation
spider_data_company
spider_data_shareholder
spider_data_senior_staff
```

分析过程包括：

- 根据项目编号、标段编号和项目名称建立项目分组；
- 识别根企业及其关联企业网络；
- 读取企业、股东、高级职员和联系方式信息；
- 规范化电话、邮箱、企业名称和人员名称；
- 比较根企业之间、根企业与关联企业之间、关联企业之间的共享信息；
- 输出风险记录及证据来源。

风险结果是关联线索，不代表违法事实认定。

## Python 环境

服务器部署使用 Python 3.10 Conda 环境：

```text
/home/wisdi/miniconda3/envs/bidding-ocr/bin/python
```

进入 Pipeline 目录：

```bash
cd /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline
```

安装 Pipeline 依赖：

```bash
/home/wisdi/miniconda3/envs/bidding-ocr/bin/python -m pip install -r requirements.txt
```

OCR 策略包也必须能够被 Pipeline 动态加载。运行参数中的 `--ocr-source` 应指向包含以下文件的目录：

```text
<ocr-source>/bidding_ocr/__init__.py
```

例如：

```text
/home/wisdi/projects/bidding_files_utils/bidding_files_ocr_strategies
```

## 配置

在以下文件中配置服务器私有变量：

```text
/home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/.env
```

`.env` 不得提交到 Git，也不得将真实密码写入 README、代码或命令历史。

数据库配置：

```dotenv
GENERAL_DB_HOST=192.168.1.210
GENERAL_DB_PORT=15400
GENERAL_DB_NAME=big_data
GENERAL_DB_USERNAME=jwmath
GENERAL_DB_PASSWORD=请填写数据库密码
GENERAL_DB_SCHEMA=dwd
GENERAL_DB_TABLE=dwd_bid_extraction_results
```

爬虫配置：

```dotenv
SPIDER_BASE_URL=http://192.168.1.166:9081
SPIDER_RESULT_DB_NAME=big_data
SPIDER_SUBMIT_MODE=single
```

Web 服务和输入安全配置：

```dotenv
BIDDING_WEB_HOST=0.0.0.0
BIDDING_WEB_PORT=8096
BIDDING_ALLOWED_INPUT_ROOTS=/home/wisdi/projects/bidding_files_utils/bidding_files
BIDDING_WEB_WORK_ROOT=/home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/web_runs
```

可选的文件大小限制：

```dotenv
BIDDING_MAX_UPLOAD_BYTES=2147483648
BIDDING_MAX_EXTRACTED_BYTES=10737418240
```

报告路径也可以显式配置：

```dotenv
BIDDING_REPORT_TEMPLATE=/home/wisdi/projects/bidding_files_utils/biding_files_risk_reports/expand_risk_reports/pipeline_risk_reports_template.md
BIDDING_REPORT_RENDERER=/home/wisdi/projects/bidding_files_utils/biding_files_risk_reports/expanded_risk_report_md_to_pdf.py
```

## CLI 使用

以下命令使用服务器 Python 环境：

```bash
PYTHON=/home/wisdi/miniconda3/envs/bidding-ocr/bin/python
PIPELINE=/home/wisdi/projects/bidding_files_utils/bidding_files_pipeline
OCR_SOURCE=/home/wisdi/projects/bidding_files_utils/bidding_files_ocr_strategies
```

### Dry-run 验证

Dry-run 只执行 PDF 解析和结果整理，不调用企业爬虫，也不写入数据库：

```bash
cd "$PIPELINE"
"$PYTHON" -m bidding_pipeline run \
  --input /home/wisdi/projects/bidding_files_utils/bidding_files \
  --output /home/wisdi/projects/bidding_files_utils/output/dry_run \
  --ocr-source "$OCR_SOURCE" \
  --workers 4 \
  --dry-run
```

### 正式运行

```bash
cd "$PIPELINE"
"$PYTHON" -m bidding_pipeline run \
  --input /home/wisdi/projects/bidding_files_utils/bidding_files \
  --output /home/wisdi/projects/bidding_files_utils/output/run_YYYYMMDD \
  --ocr-source "$OCR_SOURCE" \
  --workers 4
```

### 按类别筛选

只处理指定类别：

```bash
"$PYTHON" -m bidding_pipeline run \
  --input /path/to/input \
  --output /path/to/output \
  --ocr-source "$OCR_SOURCE" \
  --include award_notice,bid_candidates
```

排除指定类别：

```bash
"$PYTHON" -m bidding_pipeline run \
  --input /path/to/input \
  --output /path/to/output \
  --ocr-source "$OCR_SOURCE" \
  --exclude archive_info
```

`--include` 和 `--exclude` 不能同时使用。

诊断时可以跳过爬虫或风险报告：

```bash
"$PYTHON" -m bidding_pipeline run --help
```

### 重新对账

对于爬虫结果尚未完全落库的任务，可以根据已有运行目录重新对账：

```bash
"$PYTHON" -m bidding_pipeline reconcile \
  --output /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/web_runs/<job_id>/output
```

## Web 服务

手工启动：

```bash
cd /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline
/home/wisdi/miniconda3/envs/bidding-ocr/bin/python -m bidding_pipeline serve \
  --host 0.0.0.0 \
  --port 8096
```

访问：

```text
http://192.168.1.166:8096
```

Web 功能包括：

- 上传 ZIP 或选择允许范围内的服务器目录；
- 配置 OCR 类别和运行参数；
- 查看 PDF 和爬虫实时进度；
- 查看运行日志和错误信息；
- 中止当前任务；
- 重新执行历史任务；
- 下载 CSV、风险 JSON、Markdown、PDF 和运行日志。

上传 ZIP 时会校验路径穿越和符号链接，服务器本地目录必须位于 `BIDDING_ALLOWED_INPUT_ROOTS` 允许范围内。

## systemd 部署

服务模板位于：

```text
bidding_files_pipeline/deploy/bidding-pipeline-web.service
```

安装用户级服务：

```bash
mkdir -p ~/.config/systemd/user
cp /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/deploy/bidding-pipeline-web.service \
  ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now bidding-pipeline-web.service
systemctl --user status bidding-pipeline-web.service
```

查看服务日志：

```bash
journalctl --user -u bidding-pipeline-web.service -f
```

## 输出文件

每次运行的输出目录通常包含：

```text
final.csv                       # OCR 最终结果
review_queue.csv                # OCR 复核队列
run_summary.json                # OCR 运行摘要
crawl_results.json              # 爬虫任务审计和状态
spider_company_review_queue.csv # 企业名称复核队列，存在问题时生成
run_manifest.json               # 本次运行配置和结果摘要
risk_records.json               # SQL 风险分析结果
risk_report.md                  # Markdown 风险报告
risk_report.pdf                 # PDF 风险报告
```

`run_manifest.json` 会保存非敏感配置和阶段汇总，不应包含数据库密码。

## 退出码

```text
0  全部阶段成功
1  OCR 失败或发生致命入库错误
2  OCR 和入库成功，但存在爬虫失败、待对账、超时或无可见企业结果
```

退出码为 `2` 不一定表示整个 Pipeline 失败，应结合 `crawl_results.json` 的状态和 `crawlFinality` 判断。

## 测试

在服务器 Conda 环境中执行：

```bash
cd /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline
/home/wisdi/miniconda3/envs/bidding-ocr/bin/python -m pytest -q
```

测试重点覆盖 CLI、数据库、记录标准化、风险分析、爬虫调度、Pipeline runner 和 Web 接口。

## 当前架构边界

- `bidding_pipeline` 是唯一完整流程编排层。
- OCR 通过 `--ocr-source` 动态加载，不在 Pipeline 中复制 OCR 算法实现。
- 风险分析只依赖 openGauss SQL 和爬虫数据表。
- 当前部署版不依赖 Neo4j，不包含 Neo4j 图导入或风险物化流程。
- 报告生成仍依赖 `biding_files_risk_reports` 中的 Markdown 转 PDF 脚本和模板。
- 输入数据、运行日志和输出结果不应提交到源码 Git 历史。

## 常见问题

### 找不到 OCR 策略包

检查 `--ocr-source` 目录下是否存在：

```text
bidding_ocr/__init__.py
```

### 缺少数据库密码

设置：

```bash
export GENERAL_DB_PASSWORD='实际密码'
```

或在 Pipeline 目录的私有 `.env` 文件中配置。

### 风险分析没有结果

依次检查：

1. `dwd_bid_extraction_results` 是否写入了本次 `run_id`；
2. `SPIDER_RESULT_DB_NAME` 是否配置为爬虫真实落库的数据库；
3. `spider_data_company`、`spider_data_shareholder` 和 `spider_data_senior_staff` 是否存在数据；
4. `crawl_results.json` 是否仍处于待对账状态。

没有匹配到风险不等同于不存在风险，也可能是企业详情尚未落库或企业名称无法匹配。

### 报告 PDF 生成失败

检查报告模板和渲染脚本路径：

```bash
test -f /home/wisdi/projects/bidding_files_utils/biding_files_risk_reports/expanded_risk_report_md_to_pdf.py
test -f /home/wisdi/projects/bidding_files_utils/biding_files_risk_reports/expand_risk_reports/pipeline_risk_reports_template.md
```

同时确认服务器存在报告渲染所需的中文字体和 Python 依赖。

## 安全要求

- 不将数据库密码、Token 或其他密钥写入 Git、README 或日志。
- `.env` 文件权限建议设置为：

  ```bash
  chmod 600 /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/.env
  ```

- Web 服务只允许访问 `BIDDING_ALLOWED_INPUT_ROOTS` 下的本地目录。
- 生产环境应限制 8096 端口的访问来源。
- 原始招投标文件和风险报告可能包含敏感信息，应单独控制目录权限。
