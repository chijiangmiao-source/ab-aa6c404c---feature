# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

对无法遵循固定分片边界的旧型采集器，服务另提供**任意字节段续传**：按绝对偏移声明
闭区间上传，与固定分片协议在同一会话内自由交错，覆盖数据统一核算。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL`）持久化：会话、每片 SHA-256、覆盖位图（BLOB）、字节段台账
- 正文落盘：先写 `.tmp` 并 fsync，`os.replace` 就位后才在同一事务里入账
- 成品发布：按偏移从权威覆盖数据流式组装 → fsync → 校验整文件 SHA-256 → `os.replace` 原子改名

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传流程，含固定分片与任意字节段
混合链路，退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传边界 + 字节段 + 故障恢复 + 旧卷升级测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
```

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions` 表（含覆盖位图 BLOB、整文件 SHA-256、过期时间）、`chunks` 表（每片 SHA-256、大小、落盘路径）、`segments` 表（字节段起止偏移、SHA-256、落盘路径） |
| `chunks/<session_id>/00000000.chunk` | 分片正文，序号从 0 开始、8 位零填充命名 |
| `segments/<session_id>/<seg_id>.seg` | 已提交字节段正文，不可变文件，UUID 命名 |
| `artifacts/<session_id>.bin` | 校验通过后原子发布的成品 |

**权威覆盖** = 已确认分片区间 ∪ 已提交字节段区间；位图是派生缓存：某位置位当且仅当
对应逻辑分片区间已被完整覆盖，与每次覆盖变更在同一 SQLite 事务中更新。

进程启动时执行 `reconcile`：丢弃文件缺失或尺寸不符的 chunks/segments 行、删除未入账的
孤儿分片/字节段与残留 `.tmp`、按存活行重建位图 —— **已确认字节绝不误判为缺失，未确认
字节绝不误判为已收**。因此崩溃恢复是确定的：

- 正文落盘前崩溃：无任何记录，重传即新请求；
- 正文已就位、数据库提交前崩溃：孤儿文件被清理，不进入覆盖，重传重新提交；
- 数据库提交后、响应前崩溃：范围已确认，超时重传得到幂等 `200 duplicate` 结果。

## 接口约定

- 分片总数 = `ceil(file_size / chunk_size)`，序号从 `0` 开始
- 除最后一片外，每片长度必须等于 `chunk_size`；最后一片为剩余字节（`file_size - chunk_size × (total-1)`）
- 所有错误均为结构化 JSON：`{"error": {"code": "...", "message": "...", "details": {...}}}`

### 1. 建档 `POST /sessions`

```bash
curl -s -X POST http://localhost:8000/sessions \
  -H 'Content-Type: application/json' \
  -d '{
        "file_size": 4206649,
        "chunk_size": 1048576,
        "file_sha256": "b5fa3134…(整文件 SHA-256，64 位十六进制)",
        "expires_at": "2026-09-18T12:00:00+00:00"
      }'
```

`201` 响应（`total_chunks = ceil(4206649 / 1048576) = 5`）：

```json
{
  "session_id": "029c773e…",
  "status": "active",
  "file_size": 4206649,
  "chunk_size": 1048576,
  "total_chunks": 5,
  "file_sha256": "b5fa3134…",
  "received_count": 0,
  "missing_chunks": [0, 1, 2, 3, 4],
  "missing_ranges": [{"start": 0, "end": 4206648}],
  "expires_at": "2026-09-18T12:00:00+00:00",
  "created_at": "2026-09-18T03:31:21.684930+00:00",
  "completed_at": null,
  "final_sha256": null,
  "artifact_url": null
}
```

`expires_at` 必须带时区且晚于当前时间，否则 `422 SESSION_EXPIRES_IN_PAST` / `422 VALIDATION_ERROR`。

### 2. 上传分片 `PUT /sessions/{session_id}/chunks/{index}`

请求体为分片原始字节，请求头 `X-Chunk-SHA256` 携带该片摘要：

```bash
CHUNK_SHA=$(sha256sum part0.bin | cut -d' ' -f1)
curl -s -X PUT "http://localhost:8000/sessions/$SID/chunks/0" \
  -H "X-Chunk-SHA256: $CHUNK_SHA" \
  --data-binary @part0.bin
```

- 新片入库：`201`，响应含 `"duplicate": false`
- **同序号同内容重传：幂等成功** `200`，`"duplicate": true`，状态不变
- **同序号不同内容：`409 CHUNK_CONFLICT`**，已确认分片不被覆盖
- 摘要与正文不符 `400 CHUNK_DIGEST_MISMATCH`、长度不符 `400 CHUNK_SIZE_MISMATCH`、
  序号越界 `400 CHUNK_INDEX_OUT_OF_RANGE` —— 均**不记入**任何状态
- 会话过期后新分片一律 `410 SESSION_EXPIRED`（已确认分片的幂等重放仍是 `200`）
- 该分片区间已被字节段**部分**覆盖时：先对全请求做原子冲突检查，重叠字节完全一致
  才补齐其余范围（`201`）；只要有一个不同字节即 `409 RANGE_CONFLICT` 且不写入任何
  字节；区间已被相同内容**完整**覆盖时按幂等重放返回 `200 duplicate=true`

```json
{
  "session_id": "029c773e…",
  "chunk_index": 0,
  "size": 1048576,
  "sha256": "9f86d081…",
  "duplicate": false,
  "received_count": 1,
  "total_chunks": 5
}
```

### 2b. 上传任意字节段 `PUT /sessions/{session_id}/ranges`

供无法对齐固定分片边界的旧型采集器使用。请求头 `Content-Range: bytes <start>-<end>/<total>`
声明**闭区间**，`X-Range-SHA256` 声明正文摘要：

```bash
RANGE_SHA=$(sha256sum seg.bin | cut -d' ' -f1)
curl -s -X PUT "http://localhost:8000/sessions/$SID/ranges" \
  -H "Content-Range: bytes 1000000-1999999/4206649" \
  -H "X-Range-SHA256: $RANGE_SHA" \
  --data-binary @seg.bin
```

- `total` 必须等于建档 `file_size`；`end ≤ file_size - 1`；正文长度必须等于区间长度；
  单次请求区间长度不得超过 **8 MiB**
- 新增覆盖：`201`，`"duplicate": false`，`stored_ranges` 列出本次实际新写入的子区间
- 全部范围已被相同内容覆盖：`200`，`"duplicate": true`，不改动任何状态
- 与已存分片/字节段重叠时，重叠字节必须完全一致；发现第一个不同字节即
  `409 RANGE_CONFLICT`（`details.first_conflict_offset` 为首个冲突的**绝对偏移**），
  且该请求的非重叠部分也不会写入
- 边界非法 `400 INVALID_CONTENT_RANGE` / `400 RANGE_OUT_OF_BOUNDS`、总量不符
  `400 RANGE_TOTAL_MISMATCH`、摘要头非法 `400 INVALID_RANGE_DIGEST`、摘要不符
  `400 RANGE_DIGEST_MISMATCH`、长度不符 `400 RANGE_SIZE_MISMATCH`、超限
  `413 RANGE_TOO_LARGE` —— 均**不记入**任何状态
- 过期会话仅允许完全相同且不增加覆盖的重放（`200`）；任何包含新字节的请求整体
  `410 SESSION_EXPIRED`

```json
{
  "session_id": "029c773e…",
  "range": {"start": 1000000, "end": 1999999},
  "size": 1000000,
  "sha256": "9f86d081…",
  "duplicate": false,
  "stored_ranges": [{"start": 1000000, "end": 1999999}],
  "received_count": 2,
  "total_chunks": 5
}
```

尚未凑满固定逻辑分片的字节段会被持久保存（`segments` 表 + 不可变段文件），重启后
仍可继续补齐；并发提交相交区间等价于某个串行顺序：相同内容可共同形成覆盖，不同
内容只有一个提交成功，失败请求不留字节也不改进度。

### 3. 状态 `GET /sessions/{session_id}`

```bash
curl -s http://localhost:8000/sessions/$SID
```

- `missing_chunks`：**升序**缺片序号，按"逻辑分片是否已被完整覆盖"计算 —— 部分覆盖
  不算收到，完整覆盖后才从缺片列表消失
- `missing_ranges`：未覆盖的绝对字节区间列表，按起点升序、互不相交、相邻区间已合并，
  每项 `{"start": …, "end": …}` 为闭区间，旧型采集器据此直接续传
- `status` 为 `active` / `expired` / `completed`

### 4. 发布 `POST /sessions/{session_id}/finalize`

```bash
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

- 缺片：`409 CHUNKS_INCOMPLETE`（`details.missing_chunks` 列出缺片）；若已过期则 `410 SESSION_EXPIRED`
- 新旧协议合计覆盖整文件后，从权威覆盖数据（分片文件 + 字节段文件）**按偏移流式**
  组装并校验整文件 SHA-256，不复制出另一份完整输入，结果与片段到达顺序、重启、
  并发重放无关：
  - 一致 → 原子发布，`200` 返回 `final_sha256` 与 `artifact_url`；重复调用幂等
  - 不一致 → `422 INTEGRITY_MISMATCH`（`details` 含双方摘要与尺寸），**已上传字节全部保留**

```json
{
  "session_id": "029c773e…",
  "status": "completed",
  "file_size": 4206649,
  "final_sha256": "b5fa3134…",
  "artifact_size": 4206649,
  "artifact_url": "/sessions/029c773e…/artifact",
  "completed_at": "2026-09-18T03:31:21.731585+00:00"
}
```

### 5. 下载成品 `GET /sessions/{session_id}/artifact`

```bash
curl -s -OJ http://localhost:8000/sessions/$SID/artifact
sha256sum 029c773e….bin   # 与建档 file_sha256 比对即可复核
```

响应头 `X-File-SHA256` 附带成品摘要；未发布时 `409 ARTIFACT_NOT_READY`。

### 6. 健康检查 `GET /healthz`

返回 `{"status": "ok"}`，供 Compose healthcheck 与验收客户端使用。

## 错误码一览

| HTTP | code | 含义 |
|---|---|---|
| 400 | `CHUNK_INDEX_OUT_OF_RANGE` | 序号非整数或超出 `0..total-1` |
| 400 | `INVALID_CHUNK_DIGEST` | `X-Chunk-SHA256` 不是 64 位十六进制 |
| 400 | `CHUNK_SIZE_MISMATCH` | 分片长度不等于该片应有长度 |
| 400 | `CHUNK_DIGEST_MISMATCH` | 分片正文 SHA-256 与声明摘要不符 |
| 400 | `INVALID_CONTENT_RANGE` | `Content-Range` 缺失/格式非法/起点大于终点 |
| 400 | `RANGE_TOTAL_MISMATCH` | `Content-Range` 的 total 不等于建档 `file_size` |
| 400 | `RANGE_OUT_OF_BOUNDS` | 字节段终点超出文件大小 |
| 400 | `INVALID_RANGE_DIGEST` | `X-Range-SHA256` 不是 64 位十六进制 |
| 400 | `RANGE_SIZE_MISMATCH` | 正文长度不等于区间长度 |
| 400 | `RANGE_DIGEST_MISMATCH` | 字节段正文 SHA-256 与声明摘要不符 |
| 404 | `SESSION_NOT_FOUND` | 会话不存在 |
| 404 | `NOT_FOUND` | 路由不存在 |
| 409 | `CHUNK_CONFLICT` | 同序号不同内容，已确认分片不变 |
| 409 | `RANGE_CONFLICT` | 与已存分片/字节段有字节不一致，`details.first_conflict_offset` 为首个冲突绝对偏移 |
| 409 | `CHUNKS_INCOMPLETE` | 尚有缺片/缺段，不能发布 |
| 409 | `SESSION_ALREADY_COMPLETED` | 会话已完成，不可再写新内容 |
| 409 | `ARTIFACT_NOT_READY` | 成品尚未发布 |
| 410 | `SESSION_EXPIRED` | 会话已过期，仅允许完全相同且不增加覆盖的重放 |
| 413 | `RANGE_TOO_LARGE` | 单次字节段请求超过 8 MiB |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败 |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 旧版本数据卷升级

`segments` 表通过 `CREATE TABLE IF NOT EXISTS` 在建库时顺带创建：升级前已存在的
活动、过期、完成会话及其分片行原样保留，无需重传即可查询、续传、发布或下载；
不清库、不重新建档、不在启动时批量重写正文。旧卷上的会话同样获得
`missing_ranges` 状态字段与字节段续传能力。

## 测试

`pytest` 覆盖续传边界：建档参数校验、乱序上传、摘要/尺寸/越界拒绝且不入账、
同内容幂等重传、异内容 409 不污染进度、缺片状态升序、发布成功与幂等、
完整性失败保留现场、过期拒绝新内容、**重启后从已确认位置续作**（含孤儿分片/字节段与
残留临时文件清理）、结构化错误形态；字节段专项：混合协议交错续传、跨分片边界的
段、全请求原子冲突检查与首个冲突偏移、并发相交提交（同内容共同覆盖、异内容仅一方
成功）、过期/完成会话重放规则、8 MiB 上限、崩溃提交点恢复、**旧数据卷就地升级**、
大规模稀疏范围（8 MiB+ 文件、数十个散布段与单字节段）精确缺口核算与混合发布。
