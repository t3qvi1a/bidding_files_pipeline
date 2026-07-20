# 招投标 PDF 解析与企业爬取 Pipeline

该项目复用相邻 `biding_files_ocr_strategies`（也兼容服务器上的 `bidding_files_ocr_strategies`）的多进程 PDF 解析能力，并提供 CLI 与 Web 两种入口。正式任务严格按以下顺序执行：

1. 解析全部 PDF 并生成 `final.csv`；
2. 根据解析出的企业名称执行企业爬虫并等待落库；
3. 将 PDF 解析结果幂等写入 openGauss；
4. 读取本次 `run_id` 的投标结果和企业爬虫数据，分析同标段关联风险；
5. 输出风险 JSON，以文件模板生成 Markdown，再调用现有脚本渲染 PDF。

## 运行环境

- Python 3.10（与 OCR 策略的 `environment.yml` 一致）
- 相邻或显式指定的 `biding_files_ocr_strategies` 源码目录
- openGauss/PostgreSQL 兼容驱动：`psycopg2-binary`
- Web 服务：FastAPI、Uvicorn、python-multipart
- 报告生成：ReportLab、pypdf，以及 Linux/Windows 中文字体

安装依赖：

```bash
python -m pip install -r requirements.txt
python -m pip install -r ../biding_files_ocr_strategies/requirements.txt
```

在进程环境变量、当前工作目录或项目根目录中不提交的 `.env` 设置：

```bash
GENERAL_DB_PASSWORD=你的数据库密码
SPIDER_RESULT_DB_NAME=big_data
BIDDING_ALLOWED_INPUT_ROOTS=/home/wisdi/projects/bidding_files_utils/bidding_files
BIDDING_WEB_WORK_ROOT=/home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/web_runs
```

`SPIDER_RESULT_DB_NAME` 仅用于验证爬虫数据；若服务实际配置为 `big_data_demo`，将其改为 `big_data_demo`。解析结果始终写入 `big_data.dwd.dwd_bid_extraction_results`，除非显式覆盖数据库参数。

## 运行

先执行不触发爬虫、不写数据库的验证：

```bash
python -m bidding_pipeline run \
  --input /data/bidding_pdfs \
  --output /data/bidding_output \
  --ocr-source ../biding_files_ocr_strategies \
  --workers 4 \
  --dry-run
```

正式运行默认逐企业提交爬虫：

```bash
python -m bidding_pipeline run \
  --input /data/bidding_pdfs \
  --output /data/bidding_output \
  --ocr-source ../biding_files_ocr_strategies \
  --workers 4
```

如确需按单 PDF 将企业名称用英文逗号拼接为一次请求，传入 `--spider-submit-mode batch`。该模式会进入爬虫服务已知不稳定的批量分支，默认不推荐。

可使用 `--include award_notice,bid_candidates` 仅解析指定类别，或使用 `--exclude archive_info` 排除类别。两者互斥。

风险分析默认启用；仅在诊断场景可传入 `--skip-risk-analysis`。报告模板默认读取：

```text
../biding_files_risk_reports/expand_risk_reports/pipeline_risk_reports_template.md
```

该模板基于原 `risk_reports_template.md` 创建，报告正文不硬编码在 Python 中。可通过 `--report-template` 和 `--report-renderer` 覆盖路径。

## 风险分析口径

- 分析范围：本次 Pipeline `run_id` 写入的投标企业；
- 分组规则：优先使用标段编号，缺失时依次使用项目编号、项目名称与标段名称兜底；
- 数据表：`dwd_bid_extraction_results`、`spider_data_company`、`spider_data_shareholder`、`spider_data_senior_staff`；
- 风险规则：同一标段至少两家不同企业的规范化联系电话、邮箱、股东名称或高级职员名称相同；
- 删除数据：`delete_flag = 'DELETE'` 的爬虫记录不参与分析；
- 结论边界：风险记录是关联线索，不构成违法事实认定。

## Web 服务

启动局域网服务：

```bash
python -m bidding_pipeline serve --host 0.0.0.0 --port 8096
```

浏览器访问 `http://服务器IP:8096`。页面支持：

- 上传 ZIP 或填写允许范围内的服务器本地目录；
- 配置全部、include 或 exclude 文件类别；
- 查看五阶段进度、实时日志和错误信息；
- 刷新或重新打开页面后自动恢复最近任务、当前进度和已有日志；
- 使用“中止当前任务”按钮停止 Pipeline 及其 OCR 子进程；
- 使用“重新执行此任务”按钮复用服务器保存的输入和运行配置创建新任务；
- 下载解析结果 CSV、风险 JSON、PDF 报告和运行日志。

Web 后台使用单任务执行队列，避免多批 OCR 同时争抢服务器资源。每个任务在独立进程组中执行，状态和可重试配置持久化到 `web_runs/<job_id>/job_state.json`；刷新页面不会丢失记录，服务重启时未完成任务会明确显示为“已中断”。上传 ZIP 会拒绝路径穿越和符号链接，服务器路径受 `BIDDING_ALLOWED_INPUT_ROOTS` 限制。旧版任务未保存原始输入和运行配置，不能直接重试，需要重新选择 ZIP 或服务器路径。

## 输出与退出码

- OCR 原始输出：分类 CSV、`final.csv`、`review_queue.csv`、`run_summary.json`
- Pipeline 审计：`crawl_results.json`、`run_manifest.json`
- 风险与报告：`risk_records.json`、`risk_report.md`、`risk_report.pdf`
- 结果表：`dwd.dwd_bid_extraction_results`；首次写入自动创建表与唯一索引，后续按业务键 Upsert。

退出码：`0` 全部成功，`1` OCR 或致命入库错误，`2` OCR 与入库成功但存在爬虫失败、超时或无可见企业结果。

## 服务器部署

部署到 `wisdi@192.168.1.166` 时，目标工作区为 `/home/wisdi/projects/bidding_files_utils`：

```bash
scp -r biding_files_pipeline/* wisdi@192.168.1.166:/home/wisdi/projects/bidding_files_utils/bidding_files_pipeline/
scp biding_files_risk_reports/expanded_risk_report_md_to_pdf.py wisdi@192.168.1.166:/home/wisdi/projects/bidding_files_utils/biding_files_risk_reports/
scp biding_files_risk_reports/expand_risk_reports/pipeline_risk_reports_template.md wisdi@192.168.1.166:/home/wisdi/projects/bidding_files_utils/biding_files_risk_reports/expand_risk_reports/
ssh wisdi@192.168.1.166
cd /home/wisdi/projects/bidding_files_utils/bidding_files_pipeline
~/miniconda3/envs/bidding-ocr/bin/python -m pip install -r requirements.txt
chmod 600 .env
```

当前服务器已存在 Python 3.10 的 `bidding-ocr` Conda 环境，部署命令直接复用该环境；若目标服务器没有该环境，则需要由管理员安装 `python3.10-venv` 或创建等价 Conda 环境。

在服务器私有 `.env` 中配置密码后，命令行会自动加载其中未被进程环境覆盖的变量；不要把真实密码写入代码、README 或版本库。

仓库内提供 `deploy/bidding-pipeline-web.service` 用户服务模板。复制到 `~/.config/systemd/user/` 后执行：

```bash
systemctl --user daemon-reload
systemctl --user enable --now bidding-pipeline-web.service
systemctl --user status bidding-pipeline-web.service
```
