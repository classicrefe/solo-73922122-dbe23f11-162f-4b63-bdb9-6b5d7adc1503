# Point-in-time Payment Feature Builder

风控训练数据服务：按**预测当时可知**的信息重建"近 24 小时支付额"特征，避免迟到更正把未来泄漏进训练集。

技术栈：FastAPI + PostgreSQL（psycopg3）+ Docker Compose。交互式 API 文档见服务启动后的 **`/docs`**（Swagger UI）。

## 特征语义（核心规则）

对每个样本 `(account_id, prediction_at_utc)`：

1. **取预测时已可见的每笔最高修订**：在 `visible_at_utc <= prediction_at_utc`（含等于）的所有修订中，按 `payment_id` 取 `revision_id` 最大者。更正可以改账户、发生时刻、金额——一律以最高可见修订为准。
2. **撤销不恢复旧修订**：若最高可见修订 `revoked=true`，该支付**完全不计入**，不会回退到更早的修订。
3. **窗口求和**：按（更正后的）账户匹配，（更正后的）发生时刻落在 `[prediction_at_utc - 24h, prediction_at_utc)`（左闭右开）区间内的支付，对其 `amount_cents` 求和，同时给出支付笔数。

输出列：`sample_id, account_id, prediction_at_utc, amount_last_24h_cents, payment_count`。

## 快速开始

```bash
docker compose up -d --build
curl localhost:8000/health        # {"status":"ok"}
# Swagger UI: http://localhost:8000/docs
```

随后可运行示例（详见 `examples/README.md`）：

```bash
bash examples/01_late_corrections.sh     # 迟到更正 / 撤销 / 改账户 / 改时刻
bash examples/02_concurrent_uploads.sh   # 运行中并发补传 + 幂等复用 + 整批拒绝
RUN_CHUNK_DELAY_MS=100 docker compose up -d   # 放慢 worker 便于观察
bash examples/03_restart_resume.sh       # 重启接续
```

## CSV 格式

**支付修订**（`POST /payment-revision-batches`）：

```csv
payment_id,revision_id,account_id,occurred_at_utc,visible_at_utc,amount_cents,revoked
p1,1,acct-A,2026-11-15T11:00:00Z,2026-11-15T10:00:00Z,1000,false
```

- `revision_id`：非负整数，同一 `payment_id` 下越大越新。
- `occurred_at_utc` / `visible_at_utc`：ISO 8601，**必须带时区**（`Z` 或 `+08:00` 等偏移，统一归一化为 UTC）。
- `amount_cents`：64 位整数（允许负数，如退款）。
- `revoked`：`true/false/1/0/yes/no`。

**预测样本**（`POST /sample-batches`）：

```csv
sample_id,account_id,prediction_at_utc
s1,acct-A,2026-11-15T12:00:00Z
```

上传方式：原始请求体（`Content-Type: text/csv`，`curl --data-binary @file.csv`）或 multipart 表单 `file` 字段。单文件上限默认 64MB（`MAX_CSV_BYTES`）。

## 幂等与冲突

- **同号同修订同内容 → 复用**：重复上传完全相同的内容是幂等的（响应中 `rows_reused` 计数）。
- **同号异内容 → 整批拒绝**：`(payment_id, revision_id)`（或 `sample_id`）已存在且内容不同，则**整个批次回滚**（HTTP 409，响应列出冲突键），批内其他新行也不会落库。样本的幂等规则与修订一致。
- 文件内重复行：内容相同则去重，内容冲突则整批 400。

## 训练集生成语义

- `POST /training-runs` 在**受理时点**冻结水位线：仅包含此前已提交的完整样本批次。运行期间上传的批次照常受理（201），但属于**下一次**运行。
- 同一时刻只允许一个活动运行（pending/running），否则 409。
- 每个样本在**独立的短事务**中计算并持久化（`run_results` 幂等写入）：
  - **不持有长期数据库快照**，也**不为任务复制全部源历史**；as-of 重建完全依靠 `visible_at_utc <= prediction_at_utc` 时间谓词在live表上完成。
  - 服务重启（含 `docker compose restart` 或异常崩溃）后，从**已完成样本之后**接续；已完成样本不重算，后来提交的批次不混入。
- `GET /training-runs/{id}/dataset.csv` 仅在运行 **completed** 后提供（此前 409）——即全部样本的金额及其所用支付修订齐备后才可下载。已完成的运行结果不可变。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/payment-revision-batches` | 导入支付修订 CSV（201 / 400 格式错 / 409 内容冲突） |
| POST | `/sample-batches` | 导入预测样本 CSV（同上） |
| POST | `/training-runs` | 创建训练运行（201 / 409 已有活动运行） |
| GET | `/training-runs` / `/training-runs/{id}` | 运行状态与进度 |
| GET | `/training-runs/{id}/dataset.csv` | 下载特征数据集（完成前 409） |
| GET | `/sample-batches`、`/payment-revision-batches` | 批次审计列表 |
| GET | `/stats`、`/health` | 行数统计 / 健康检查 |

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_URL` | `postgresql://app:app@localhost:5432/appdb` | 连接串 |
| `RUN_CHUNK_SIZE` | `200` | worker 每批抓取的样本数 |
| `RUN_CHUNK_DELAY_MS` | `0` | 每批间人为延时（演示/测试用） |
| `RUNNER_IDLE_SLEEP_MS` | `500` | 无任务时轮询间隔 |
| `MAX_CSV_BYTES` | `67108864` | 单 CSV 上限 |

## 本地开发与测试

```bash
pip install -r requirements.txt pytest
# 需要一个可连的 PostgreSQL 16（测试库会自动重建 schema）
TEST_DATABASE_URL=postgresql://app@127.0.0.1:5544/appdb_test2 \
TEST_ADMIN_URL=postgresql://app@127.0.0.1:5544/postgres \
    pytest tests/ -q
```

- `tests/test_csvio.py`：CSV 解析/校验单元测试（无需服务）。
- `tests/test_e2e.py`：真实 PostgreSQL 上的端到端测试——窗口与可见性边界、撤销语义、账户/时刻更正、幂等复用、整批拒绝、水位线隔离、下载门槛、优雅重启与崩溃接续。

## 设计说明与限制

- **单副本**：worker 在应用进程内（单实例）。多副本部署需要为 worker 加选主，未实现。
- **结果固化**：样本在某次运行中计算后即固化；运行中途到达的"回填过去可见时间"的修订只影响尚未计算的样本（这是"不持快照"设计的固有取舍）。如需重算，用新 `sample_id` 提交样本并创建新运行。
- **规模**：每样本一次 as-of 查询（`visible_at` 索引 + 主键）；百万级修订、十万级样本量级可正常工作。更大规模可考虑分区与物化中间层，未实现。
- 上传解析为整文件内存处理（64MB 上限内）。
