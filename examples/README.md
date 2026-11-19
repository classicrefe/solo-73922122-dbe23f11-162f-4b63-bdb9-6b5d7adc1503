# 示例

先启动服务（首次建议清空数据卷以获得确定性输出）：

```bash
docker compose down -v
docker compose up -d --build
```

所有脚本用 `BASE` 环境变量定位 API（默认 `http://localhost:8000`），依赖 `curl` 与 `python3`。

## 01 · 迟到更正（`01_late_corrections.sh`）

时间线（`T = 2026-11-15T12:00:00Z`）：

1. 导入基础修订 `data/revisions_base.csv`（T 之前已可见），提交样本 `s1(A@T)`、`s2(B@T)`，创建运行 1：
   - `s1 = 1300`（p1 的 1000 + p4 的 300；p2 发生在 T-25h，在窗口外）
   - `s2 = 700`
2. 导入迟到修订 `data/revisions_late.csv`（多数 `visible_at = T+1h`）：
   - `p1#2`：更正把账户 A→C
   - `p4#2`：撤销（`revoked=true`）
   - `p2#2`：更正把发生时刻从 T-25h 移入窗口（T-5h）
   - `p5#1`：晚到的新支付
   - `p6#1`：**回填**型修订——`visible_at = T-5h`（T 时本可知，只是迟到入库）
3. 提交 `s3(A@T+2h)`、`s4(C@T+2h)`、`s5(A@T)`，创建运行 2：
   - `s3 = 6249`（5000 + 250 + 999；p1 已归 C、p4 已撤销）
   - `s4 = 1000`（p1 更正后属于 C）
   - `s5 = 2299`：与 s1 同一预测时刻，仍**不包含** T+1h 才可见的更正，但包含回填的 p6 → 1300 + 999
4. 运行 1 的结果集不可变：`s1` 在其中永远是 1300。

## 02 · 并发补传 / 幂等 / 冲突（`02_concurrent_uploads.sh`）

1. 上传 5000 个样本并创建运行；**运行进行中**再上传 50 个样本——受理成功（201）但不计入本次运行（水位线固定）。
2. 原样重传 `revisions_base.csv` → 全部 `rows_reused`（幂等复用）。
3. 上传 `revisions_conflict.csv`（`p1#1` 金额被改）→ HTTP 409，**整批拒绝**，其中的新支付 `p9` 也不落库。
4. 运行 1 完成后的数据集不含 `ex2-extra-*`；创建运行 2 后包含这 50 个样本。

提示：想放大可观察窗口，用 `RUN_CHUNK_DELAY_MS=50 docker compose up -d` 放慢 worker。

## 03 · 重启接续（`03_restart_resume.sh`）

1. 上传 20000 个样本并创建运行，轮询到部分完成（`done_samples` 介于 0 与 total 之间）。
2. `docker compose restart app`。
3. 重启后 `done_samples` **不回退**（从已完成样本接续），最终数据集行数 == `total_samples`，且无重复样本。

提示：worker 默认很快，20000 样本可能几秒内跑完；用 `RUN_CHUNK_DELAY_MS=100 docker compose up -d` 让中断窗口清晰可见。若脚本提示"run completed before restart"，按此调整即可。

## 04 · 渠道补传回执（`04_reupload_partial.sh`）

新入口 `POST /payment-revision-reuploads` 与原入口接收同样的 CSV，但冲突处理改为**按支付拒收**：

1. 先确保 `revisions_base.csv` 已入库（幂等复用）。
2. 上传 `data/revisions_reupload.csv`：
   - `p1#1` 金额与库内不符（7777 vs 1000）→ **p1 在本批的全部修订被拒收**（含新修订 `p1#3`）
   - `p8#1` 文件内同键异值（250/260）→ **p8 全部拒收**
   - `p7#1` 新键写入、`p2#1` 同内容复用——正常支付照常入库
3. 按批次号查回执 `GET /payment-revision-reuploads/{batch_id}`：写入/复用/拒收数量、冲突键（含原因与双方内容）。
4. 下载待修正 CSV `GET /payment-revision-reuploads/{batch_id}/rejected.csv`：保留被拒支付在本批的**全部源行**。
5. 修正该 CSV（p1 金额改回 1000、p8 去掉冲突行）后重传 → 全部写入/复用。
6. 原入口依旧整批拒绝（409），`p9` 不落库。

字段格式错误（如时间戳非法）时两个入口都是**整批拒收**（400），且不留回执。

## 数据文件

| 文件 | 内容 |
|---|---|
| `data/revisions_base.csv` | 4 条基础修订（p1–p4，T 前可见） |
| `data/revisions_late.csv` | 5 条迟到修订（更正账户/时刻、撤销、晚到、回填） |
| `data/revisions_conflict.csv` | 与既有 `p1#1` 内容冲突的批次（含一条新行 p9，用于证明整批回滚） |
| `data/revisions_reupload.csv` | 补传批次：p1 与库内冲突、p8 文件内冲突、p7 新增、p2 复用 |
| `data/samples_1.csv` / `data/samples_2.csv` | 示例 01 的样本 |
