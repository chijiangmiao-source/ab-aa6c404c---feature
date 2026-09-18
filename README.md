# 轮轴超声探伤 · 分片续传上传服务

面向车间不稳定网络的大文件上传后端：检测设备把扫描文件切成固定大小的分片乱序上传，
任意中断后只需补传缺片；服务重启不丢已确认进度；全部到齐且整文件 SHA-256 校验通过后，
成品文件被**原子发布**，否则保留现场并返回结构化错误。

对于**无法对齐固定分片边界的旧型采集器**，服务同时支持"任意字节段续传"：
`PUT /sessions/{id}/ranges` 以 `Content-Range` 声明闭区间即可。两种协议可在同一会话中
任意交错，重叠字节必须逐字节一致，覆盖权威数据为"已确认固定分片 ∪ 已确认字节段"。

- Python 3.13 + FastAPI，纯后端，无前端
- SQLite（WAL + `synchronous=FULL`）持久化：会话、每片 SHA-256、接收位图（BLOB）、字节段
- 正文落盘：先写 `.tmp` 并 fsync，`os.replace` 就位后才提交元数据
- 字节段仅保存"尚未覆盖的切片"（不可变 `.seg`），重启后继续补齐
- 成品发布：从权威覆盖直接按偏移流式组装 → fsync → 校验整文件 SHA-256 → `os.replace` 原子改名

## 快速开始（Docker Compose）

```bash
docker compose up --build api                 # 宿主端口默认 8000
API_PORT=9000 docker compose up --build api   # API_PORT 覆盖宿主端口
```

一次性验收服务 `verify`（等待 API 健康后跑完整续传流程：固定分片、字节段混传、
冲突、8 MiB 上限等；退出码 0 = 通过）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
docker compose down
```

## 本地开发

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                        # 续传边界测试
DATA_DIR=./data uvicorn app.asgi:app --port 8000
```

## 数据布局与重启语义

`DATA_DIR`（容器内 `/data`，本地默认 `./data`）：

| 路径 | 内容 |
|---|---|
| `db.sqlite3` | `sessions` 表（接收位图 BLOB、整文件 SHA-256、过期时间）、`chunks` 表（每片 SHA-256、大小、落盘路径）、`ranges` 表（每个字节段切片的起止偏移、SHA-256、落盘路径） |
| `chunks/<session_id>/00000000.chunk` | 固定分片正文，序号从 0 开始、8 位零填充命名 |
| `segments/<session_id>/<16位十六进制起点>-<token>.seg` | 字节段请求中**此前未覆盖**的连续切片（不可变，只增不改） |
| `artifacts/<session_id>.bin` | 校验通过后原子发布的成品 |

进程启动时执行 `reconcile`：丢弃文件缺失或尺寸不符的 `chunks`/`ranges` 行、删除未入账
的孤儿正文与残留 `.tmp`、按存活行重建位图 —— **已确认字节绝不误判为缺失，未确认字节
绝不误判为已收**。升级前创建的数据库在首次打开时以 `CREATE TABLE IF NOT EXISTS` 平滑
增加 `ranges` 表，不复制、不重写任何既有正文；活动、过期、完成会话均可直接查询、续传、
发布与下载。

### 提交协议（正文与 SQLite 无法共用事务）

1. 请求正文流式写入会话目录下的 `.tmp`（边写边算长度与 SHA-256），fsync 后关闭；
2. 长度、摘要、边界校验通过后，在进程内锁内**逐字节比对全部重叠区**；
3. 冲突 → 整体拒绝（`409 RANGE_CONFLICT`，返回首个冲突绝对偏移），不发布任何切片；
4. 无冲突 → 仅把未覆盖切片各复制成一个 fsync + `os.replace` 就位的不可变 `.seg`；
5. 切片全部耐久后才在**一个 SQLite 事务**里插入 `ranges` 行并更新位图；
6. 元数据提交失败时删除刚就位的切片；响应前崩溃也安全：已提交即权威，未提交的文件
   由下次启动 reconcile 当作孤儿清除。超时重试因此总是得到与首次提交一致的幂等结果。

## 接口约定

- 分片总数 = `ceil(file_size / chunk_size)`，序号从 `0` 开始
- 除最后一片外，每片长度必须等于 `chunk_size`；最后一片为剩余字节（`file_size - chunk_size × (total-1)`）
- 字节段 `Content-Range: bytes start-end/total` 为**闭区间**；`total` 必须等于建档 `file_size`，
  正文长度必须等于 `end-start+1`，单次请求最多 **8 MiB**
- 状态响应中的 `missing_chunks` 按"逻辑分片是否被**完整**覆盖"计算（部分覆盖不算收到），
  `missing_ranges` 为按起点升序、互不相交且已合并相邻区间的缺口列表（闭区间）
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

### 2. 上传固定分片 `PUT /sessions/{session_id}/chunks/{index}`

请求体为分片原始字节，请求头 `X-Chunk-SHA256` 携带该片摘要：

```bash
CHUNK_SHA=$(sha256sum part0.bin | cut -d' ' -f1)
curl -s -X PUT "http://localhost:8000/sessions/$SID/chunks/0" \
  -H "X-Chunk-SHA256: $CHUNK_SHA" \
  --data-binary @part0.bin
```

- 新片入库：`201`，响应含 `"duplicate": false`
- **同序号同内容重传：幂等成功** `200`，`"duplicate": true`，状态不变
- 同序号已有不同内容的固定分片：`409 CHUNK_CONFLICT`，已确认分片不被覆盖
- 与已保存字节段重叠时执行与字节段相同的**全请求原子冲突检查**：一致则补齐其余范围
  （沿用 201/200 幂等规则），发现任一不同字节则 `409 RANGE_CONFLICT`（`details.conflict_offset`
  为首个冲突绝对偏移），本请求的非重叠部分也不落盘
- 摘要与正文不符 `400 CHUNK_DIGEST_MISMATCH`、长度不符 `400 CHUNK_SIZE_MISMATCH`、
  序号越界 `400 CHUNK_INDEX_OUT_OF_RANGE` —— 均**不记入**任何状态
- 会话过期后新字节一律 `410 SESSION_EXPIRED`（已确认内容的幂等重放仍是 `200`）

### 3. 上传任意字节段 `PUT /sessions/{session_id}/ranges`

供无法对齐固定分片边界的旧型采集器使用。请求头 `Content-Range` 声明闭区间、
`X-Range-SHA256` 声明正文摘要：

```bash
SEG_SHA=$(sha256sum part.bin | cut -d' ' -f1)
curl -s -X PUT "http://localhost:8000/sessions/$SID/ranges" \
  -H "Content-Range: bytes 1048570-2097155/4206649" \
  -H "X-Range-SHA256: $SEG_SHA" \
  --data-binary @part.bin
```

- 请求确实增加覆盖：`201`，`"duplicate": false`
- 全部范围已被**相同内容**覆盖：`200`，`"duplicate": true`（状态不变；过期/完成后仍允许）
- 与既有固定分片或字节段存在重叠且任一字节不同：`409 RANGE_CONFLICT`，
  `details.conflict_offset` 为首个冲突绝对偏移，**整个请求**（含非重叠部分）不写入
- `total ≠ file_size` → `400 RANGE_TOTAL_MISMATCH`；边界非法（`start>end` 或
  `end ≥ total`）→ `400 RANGE_OUT_OF_BOUNDS`；正文长度不符 → `400 RANGE_SIZE_MISMATCH`；
  摘要非法/不符 → `400 INVALID_RANGE_DIGEST` / `400 RANGE_DIGEST_MISMATCH`；
  头格式无法解析 → `400 INVALID_CONTENT_RANGE`；缺头 → `422 VALIDATION_ERROR`
- 正文超过 8 MiB：流式接收超限即 `413 RANGE_TOO_LARGE`，不落盘、不记账
- 过期会话只接受**完全相同且不增加覆盖**的重放；任何包含新字节的请求整体 `410 SESSION_EXPIRED`
- 并发提交相交区间等价于某一串行顺序：相同内容共同形成覆盖，不同内容仅一个提交成功，
  失败请求不遗留任何字节或进度

```json
{
  "session_id": "029c773e…",
  "start": 1048570,
  "end": 2097155,
  "size": 1048586,
  "sha256": "9f86d081…",
  "duplicate": false,
  "received_count": 1,
  "total_chunks": 5,
  "missing_ranges": [{"start": 0, "end": 1048569}, {"start": 2097156, "end": 4206648}]
}
```

### 4. 状态 `GET /sessions/{session_id}`

```bash
curl -s http://localhost:8000/sessions/$SID
```

`missing_chunks` 为**升序**缺片序号（分片被固定分片或字节段完整覆盖后才消失）；
`missing_ranges` 为升序、互不相交、相邻已合并的缺口（闭区间），检测设备可任选一种
补传；`status` 为 `active` / `expired` / `completed`。原有字段全部保留。

### 5. 发布 `POST /sessions/{session_id}/finalize`

```bash
curl -s -X POST http://localhost:8000/sessions/$SID/finalize
```

- 尚有逻辑分片未完整覆盖：`409 CHUNKS_INCOMPLETE`（`details.missing_chunks` 列出缺片）；
  若已过期则 `410 SESSION_EXPIRED`
- 覆盖完整后，服务**直接从权威覆盖数据按偏移生成顺序读盘计划并流式组装**（不会先复制
  出第二份完整输入；不逐字节建库；上传读取量只与本次正文及其实际重叠区有关），随后校验
  整文件 SHA-256：
  - 一致 → 原子发布，`200` 返回 `final_sha256` 与 `artifact_url`；重复调用幂等
  - 不一致 → `422 INTEGRITY_MISMATCH`（`details` 含双方摘要与尺寸），**全部进度保留**
- 组装结果与片段到达顺序、重启、并发重放无关，始终确定

### 6. 下载成品 `GET /sessions/{session_id}/artifact`

```bash
curl -s -OJ http://localhost:8000/sessions/$SID/artifact
sha256sum 029c773e….bin   # 与建档 file_sha256 比对即可复核
```

响应头 `X-File-SHA256` 附带成品摘要；未发布时 `409 ARTIFACT_NOT_READY`。

### 7. 健康检查 `GET /healthz`

返回 `{"status": "ok"}`，供 Compose healthcheck 与验收客户端使用。

## 错误码一览

| HTTP | code | 含义 |
|---|---|---|
| 400 | `CHUNK_INDEX_OUT_OF_RANGE` | 序号非整数或超出 `0..total-1` |
| 400 | `INVALID_CHUNK_DIGEST` | `X-Chunk-SHA256` 不是 64 位十六进制 |
| 400 | `CHUNK_SIZE_MISMATCH` | 分片长度不等于该片应有长度 |
| 400 | `CHUNK_DIGEST_MISMATCH` | 分片正文 SHA-256 与声明摘要不符 |
| 400 | `INVALID_CONTENT_RANGE` | `Content-Range` 头无法解析为 `bytes start-end/total` |
| 400 | `RANGE_TOTAL_MISMATCH` | `Content-Range` 的 total 不等于建档文件大小 |
| 400 | `RANGE_OUT_OF_BOUNDS` | 字节段边界越过文件范围或 `start>end` |
| 400 | `RANGE_SIZE_MISMATCH` | 字节段正文长度不等于 `end-start+1` |
| 400 | `INVALID_RANGE_DIGEST` | `X-Range-SHA256` 不是 64 位十六进制 |
| 400 | `RANGE_DIGEST_MISMATCH` | 字节段正文 SHA-256 与声明摘要不符 |
| 404 | `SESSION_NOT_FOUND` | 会话不存在 |
| 404 | `NOT_FOUND` | 路由不存在 |
| 409 | `CHUNK_CONFLICT` | 同序号固定分片不同内容，已确认分片不变 |
| 409 | `RANGE_CONFLICT` | 重叠字节不一致，`details.conflict_offset` 为首个冲突绝对偏移，整请求未落盘 |
| 409 | `CHUNKS_INCOMPLETE` | 尚有逻辑分片未完整覆盖，不能发布 |
| 409 | `SESSION_ALREADY_COMPLETED` | 会话已完成，不可再写入新字节 |
| 409 | `ARTIFACT_NOT_READY` | 成品尚未发布 |
| 410 | `SESSION_EXPIRED` | 会话已过期，仅接受不增加覆盖的相同重放 |
| 413 | `RANGE_TOO_LARGE` | 单次字节段请求超过 8 MiB |
| 422 | `VALIDATION_ERROR` | 请求体/请求头校验失败 |
| 422 | `SESSION_EXPIRES_IN_PAST` | 建档过期时间不晚于当前时间 |
| 422 | `INTEGRITY_MISMATCH` | 组装结果与建档摘要不符，现场保留 |
| 500 | `INTERNAL_ERROR` | 未预期错误 |

## 测试

`pytest` 覆盖续传边界：建档参数校验、乱序上传、摘要/尺寸/越界拒绝且不入账、
同内容幂等重传、异内容 409 不污染进度、缺片/缺口状态升序与相邻合并、
**固定分片与任意字节段交错续传**、双向全请求原子冲突（含首个冲突绝对偏移）、
部分覆盖不完成分片、过期仅允许相同重放、完成会话只接受幂等重放、
**重启后从已确认位置续作**（含孤儿正文与残留临时文件清理、提交窗口崩溃的确定性）、
并发相交区间的串行等价（相同内容合并、不同内容单一赢家）、
**旧卷就地升级后续传/发布/下载**、大规模稀疏范围（不逐字节建库、不整盘扫描）、
24 MiB 混合流式组装与发布、结构化错误形态。
